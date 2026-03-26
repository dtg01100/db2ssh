# ibm-db-ssh

Query IBM i Db2 databases over SSH — no `ibm_db` driver required.

## Why

The `ibm_db` Python driver bundles IBM's proprietary ODBC/CLI libraries, which creates licensing friction, installation headaches, and platform compatibility issues. This project takes a different approach: connect to the IBM i via SSH and use its built-in `db2` command-line tool.

## Requirements

- Python 3.7+
- [paramiko](https://www.paramiko.org/) (installed automatically with pip)
- SSH access to the target IBM i system

## Installation

```
pip install paramiko
```

Then copy `ibm_db_ssh.py` and/or `ssh_query_runner.py` into your project.

## Usage

### DB-API 2.0 Driver

Drop-in replacement pattern for code that expects a PEP 249 database interface:

```python
from ibm_db_ssh import connect

conn = connect(host="your-ibm-i.example.com", user="myuser", password="mypass")

cur = conn.cursor()
cur.execute("SELECT TABLE_NAME, TABLE_TYPE FROM QSYS2.SYSTABLES WHERE TABLE_TYPE = ? FETCH FIRST 10 ROWS ONLY", ['L'])

for row in cur.fetchall():
    print(row)

conn.close()
```

With context manager:

```python
with connect(host="your-ibm-i.example.com", user="myuser", password="mypass") as conn:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM QSYS2.SYSTABLES")
    print(cur.fetchone())
```

With SSH key:

```python
conn = connect(host="your-ibm-i.example.com", user="myuser", key_filename="~/.ssh/id_rsa")
```

Without password or key, paramiko will try SSH agent and default keys (`~/.ssh/id_rsa`, etc.):

```python
conn = connect(host="your-ibm-i.example.com", user="myuser")
```

### CLI Tool

```bash
# Password auth
python ssh_query_runner.py --host your-ibm-i.example.com --user myuser --password mypass \
  --query "SELECT * FROM QSYS2.SYSTABLES FETCH FIRST 5 ROWS ONLY"

# Key-based auth
python ssh_query_runner.py --host your-ibm-i.example.com --user myuser \
  --key-file ~/.ssh/id_rsa \
  --query "SELECT * FROM QSYS2.SYSTABLES FETCH FIRST 5 ROWS ONLY"

# From a SQL file
python ssh_query_runner.py --host your-ibm-i.example.com --user myuser --password mypass \
  --file queries.sql

# Save output to file
python ssh_query_runner.py --host your-ibm-i.example.com --user myuser --password mypass \
  --query "SELECT * FROM QSYS2.SYSTABLES" --output results.txt

# Password via environment variable
export DB2_PASSWORD=mypass
python ssh_query_runner.py --host your-ibm-i.example.com --user myuser \
  --query "SELECT CURRENT_DATE FROM SYSIBM.SYSDUMMY1"
```

## How It Works

1. Opens an SSH connection to the IBM i using paramiko
2. Writes the SQL statement to a temporary file on the remote system
3. Executes the query via `qsh -c "db2 -f <file>"`
4. Parses the fixed-width columnar output into structured results
5. Cleans up the remote temp file

No software needs to be installed on the IBM i beyond its default SSH server and the built-in `db2` command.

## DB-API 2.0 Compliance

| Feature | Status |
|---------|--------|
| `connect()` | Implemented |
| `Connection.cursor()` | Implemented |
| `Cursor.execute()` | Implemented |
| `Cursor.executemany()` | Implemented |
| `Cursor.fetchone()` | Implemented |
| `Cursor.fetchmany()` | Implemented |
| `Cursor.fetchall()` | Implemented |
| Iterator protocol | Implemented |
| Context manager | Implemented |
| `?` (qmark) parameter style | Implemented |
| Error hierarchy | Implemented |
| `commit()` | No-op (IBM i autocommit) |
| `rollback()` | Raises `NotSupportedError` |

## Error Handling

All errors from the IBM i are raised as DB-API 2.0 exceptions:

```python
from ibm_db_ssh import connect, ProgrammingError, OperationalError

try:
    conn = connect(host="bad-host", user="user", password="pass")
except OperationalError as e:
    print(f"Connection failed: {e}")

try:
    cur.execute("SELECT * FROM NONEXISTENT.TABLE")
except ProgrammingError as e:
    print(f"Query failed: {e}")
```

## Security Notes

- **Authentication**: Supports password auth and SSH key-based auth. Key auth is recommended for production — use `--key-file` or pass `key_filename` to `connect()`. If no password or key is specified, paramiko will try your SSH agent and default keys (`~/.ssh/id_rsa`, etc.).
- **Passwords**: Use the `DB2_PASSWORD` environment variable or interactive prompt rather than passing passwords on the command line.
- **Host key verification**: The default configuration auto-accepts unknown SSH host keys (`AutoAddPolicy`). For production use, consider implementing strict host key checking.
- **Temp files**: A uniquely-named SQL file (`/tmp/.db2ssh_<uuid>.sql`) is written to the IBM i during each query execution and deleted immediately after. The UUID-based name prevents collision between concurrent executions and eliminates symlink-based TOCTOU attacks.

## Limitations

- Result sets only: DDL/DML statements that don't return rows will execute but won't report affected row counts accurately.
- `NULL` literal: Bare `SELECT NULL` may not work on some IBM i versions. Use `CAST(NULL AS <type>)` instead.
- No transactions: `commit()` is a no-op, `rollback()` raises `NotSupportedError`.
- Single-threaded: Each `Connection` object holds one SSH session. Share across threads at the module level only (`threadsafety=1`).
- No SSL/TLS: SSH transport is encrypted, but the db2 connection from the IBM i to its own database is local.

## License

MIT
