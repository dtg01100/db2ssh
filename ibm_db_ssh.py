"""
DB-API 2.0 (PEP 249) driver for IBM i Db2 over SSH.

Uses the qsh 'db2' command on the IBM i via paramiko — no ibm_db needed.

Usage:
    from ibm_db_ssh import connect
    conn = connect(host="your-ibm-i.example.com", user="myuser", password="mypass")
    cur = conn.cursor()
    cur.execute("SELECT TABLE_NAME FROM QSYS2.SYSTABLES FETCH FIRST 5 ROWS ONLY")
    for row in cur.fetchall():
        print(row)
    conn.close()
"""

import paramiko
import uuid

# DB-API 2.0 module-level attributes
apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "qmark"  # Use ? for parameters (we'll implement substitution)


# DB-API 2.0 exceptions
class Warning(Exception):
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


def _parse_db2_output(output):
    """Parse db2 -S output into (description, rows).

    description: list of column name strings
    rows: list of tuples of string values
    """
    lines = output.splitlines()

    # Find the separator line (all dashes and spaces)
    sep_idx = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped and all(c in "- " for c in stripped):
            sep_idx = i
            break

    if sep_idx is None:
        return [], []

    sep_line = lines[sep_idx]
    header_line = lines[sep_idx - 1] if sep_idx > 0 else ""

    # Extract column start positions from separator line
    col_starts = []
    i = 0
    while i < len(sep_line):
        if sep_line[i] == "-":
            col_starts.append(i)
            while i < len(sep_line) and sep_line[i] == "-":
                i += 1
        else:
            i += 1

    if not col_starts:
        return [], []

    # Build column boundary slices: (start, end_or_None)
    col_slices = []
    for j, start in enumerate(col_starts):
        end = col_starts[j + 1] if j + 1 < len(col_starts) else None
        col_slices.append((start, end))

    # Extract column names from header line
    description = []
    for start, end in col_slices:
        name = header_line[start:end].strip() if start < len(header_line) else ""
        description.append(name)

    # Extract data rows (everything between separator and trailing whitespace/count)
    rows = []
    for line in lines[sep_idx + 1 :]:
        stripped = line.strip()
        if not stripped:
            continue
        if "RECORD(S) SELECTED" in stripped:
            continue
        row = []
        for start, end in col_slices:
            val = line[start:end].strip() if start < len(line) else ""
            row.append(val)
        rows.append(tuple(row))

    return description, rows


def _parse_error(output):
    """Extract error message from db2 error output."""
    lines = output.strip().splitlines()
    messages = []
    for line in lines:
        line = line.strip()
        if line.startswith("SQLSTATE:") or line.startswith("NATIVE ERROR"):
            messages.append(line)
        elif "not found" in line.lower() or "error" in line.lower():
            messages.append(line)
    return "; ".join(messages) if messages else output.strip()


def _qmark_to_positional(sql, params):
    """Replace ? placeholders with positional format for safe shell escaping."""
    if not params:
        return sql, []
    parts = sql.split("?")
    if len(parts) - 1 != len(params):
        raise ProgrammingError(
            f"Expected {len(parts) - 1} parameters, got {len(params)}"
        )
    # Build the SQL with literal values (qsh db2 doesn't support parameterized queries natively)
    result = parts[0]
    for i, param in enumerate(params):
        if param is None:
            result += "NULL"
        elif isinstance(param, (int, float)):
            result += str(param)
        elif isinstance(param, str):
            escaped = param.replace("'", "''")
            result += f"'{escaped}'"
        else:
            escaped = str(param).replace("'", "''")
            result += f"'{escaped}'"
        result += parts[i + 1]
    return result, []


class Cursor:
    """DB-API 2.0 cursor for IBM i Db2 over SSH."""

    description = None  # PEP 249: sequence of 7-item sequences after execute()
    rowcount = -1  # PEP 249: number of rows produced/affected
    arraysize = 1  # PEP 249: default fetchmany size

    def __init__(self, connection):
        self._connection = connection
        self._rows = []
        self._pos = 0
        self.description = None
        self.rowcount = -1

    def execute(self, operation, parameters=None):
        if self._connection._closed:
            raise InterfaceError("Connection is closed")

        if parameters:
            operation, _ = _qmark_to_positional(operation, parameters)

        ssh = self._connection._ssh
        remote_sql = f"/tmp/.db2ssh_{uuid.uuid4().hex}.sql"

        escaped_sql = operation.replace("\\", "\\\\").replace("'", "'\\''")
        ssh.exec_command(f"printf '%s\\n' '{escaped_sql}' > {remote_sql}")

        try:
            cmd = f'qsh -c "db2 -f {remote_sql}"'
            stdin, stdout, stderr = ssh.exec_command(cmd)
            output = stdout.read().decode()
            exit_status = stdout.channel.recv_exit_status()
        finally:
            # Always clean up the temp file, even if the query fails
            ssh.exec_command(f"rm -f {remote_sql}")

        if exit_status != 0:
            msg = _parse_error(output)
            raise ProgrammingError(msg)

        desc, rows = _parse_db2_output(output)

        # Build PEP 249 description: (name, type_code, display_size, internal_size, precision, scale, null_ok)
        self.description = (
            [(name, None, None, None, None, None, None) for name in desc]
            if desc
            else None
        )
        self._rows = rows
        self._pos = 0
        self.rowcount = len(rows)
        return self

    def executemany(self, operation, seq_of_parameters):
        for params in seq_of_parameters:
            self.execute(operation, params)

    def fetchone(self):
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        result = self._rows[self._pos : self._pos + size]
        self._pos += len(result)
        return result

    def fetchall(self):
        result = self._rows[self._pos :]
        self._pos = len(self._rows)
        return result

    def close(self):
        self._connection = None
        self._rows = []
        self.description = None

    def setinputsizes(self, sizes):
        pass  # Not applicable

    def setoutputsize(self, size, column=None):
        pass  # Not applicable

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class Connection:
    """DB-API 2.0 connection for IBM i Db2 over SSH."""

    def __init__(
        self, host, user, password=None, key_filename=None, port=22, timeout=10
    ):
        self._host = host
        self._user = user
        self._closed = False
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self._ssh.connect(
                host,
                port=port,
                username=user,
                password=password,
                key_filename=key_filename,
                timeout=timeout,
            )
        except paramiko.AuthenticationException:
            raise OperationalError(f"Authentication failed for {user}@{host}")
        except paramiko.SSHException as e:
            raise OperationalError(f"SSH connection failed: {e}")
        except Exception as e:
            raise OperationalError(f"Connection failed: {e}")

    def cursor(self):
        if self._closed:
            raise InterfaceError("Connection is closed")
        return Cursor(self)

    def commit(self):
        pass  # IBM i autocommit behavior depends on job config

    def rollback(self):
        raise NotSupportedError("Rollback not supported over SSH db2 interface")

    def close(self):
        if not self._closed:
            self._ssh.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def connect(
    host, user=None, password=None, key_filename=None, port=22, timeout=10, **kwargs
):
    """Connect to IBM i Db2 via SSH.

    Returns a DB-API 2.0 Connection object.

    Authentication is attempted in this order:
        1. Password (explicit or DB2_PASSWORD env var)
        2. SSH agent
        3. Default key files (~/.ssh/id_rsa, etc.)
        4. Explicit key_filename

    Parameters:
        host: IBM i hostname or IP
        user: IBM i username
        password: IBM i password (or set DB2_PASSWORD env var)
        key_filename: path to private key file for key-based auth
        port: SSH port (default 22)
        timeout: connection timeout in seconds (default 10)
    """
    import os

    if password is None:
        password = os.environ.get("DB2_PASSWORD")
    if password == "":
        password = None
    if not user:
        raise InterfaceError("user is required")
    if not password and not key_filename:
        # Let paramiko try agent and default keys
        pass
    return Connection(
        host,
        user,
        password=password,
        key_filename=key_filename,
        port=port,
        timeout=timeout,
    )
