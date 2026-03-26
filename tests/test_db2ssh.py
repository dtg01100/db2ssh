"""Tests for db2ssh — no IBM i connection required."""

import os
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from db2ssh import (
    _parse_db2_output,
    _parse_error,
    _qmark_to_positional,
    connect,
    Connection,
    Cursor,
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    OperationalError,
    ProgrammingError,
    IntegrityError,
    DataError,
    NotSupportedError,
    apilevel,
    threadsafety,
    paramstyle,
)


# --- Module Attributes ---


class TestModuleAttributes:
    def test_apilevel(self):
        assert apilevel == "2.0"

    def test_threadsafety(self):
        assert threadsafety == 1

    def test_paramstyle(self):
        assert paramstyle == "qmark"


# --- Exception Hierarchy ---


class TestExceptions:
    def test_warning(self):
        assert issubclass(Warning, Exception)

    def test_error_hierarchy(self):
        assert issubclass(Error, Exception)
        assert issubclass(InterfaceError, Error)
        assert issubclass(DatabaseError, Error)
        assert issubclass(OperationalError, DatabaseError)
        assert issubclass(ProgrammingError, DatabaseError)
        assert issubclass(IntegrityError, DatabaseError)
        assert issubclass(DataError, DatabaseError)
        assert issubclass(NotSupportedError, DatabaseError)


# --- Output Parsing ---


class TestParseDb2Output:
    def test_two_columns(self):
        output = (
            "\nTABLE_NAME                                                        "
            "                                                                "
            "TABLE_TYPE \n"
            "----------------------------------------------------------------"
            "----------------------------------------------------------------"
            " -----------\n"
            "BROOKFL0                                                          "
            "                                                                "
            "L          \n"
            "BROOKRL0                                                          "
            "                                                                "
            "L          \n"
            "\n  2 RECORD(S) SELECTED.\n\n"
        )
        desc, rows = _parse_db2_output(output)
        assert desc == ["TABLE_NAME", "TABLE_TYPE"]
        assert rows == [("BROOKFL0", "L"), ("BROOKRL0", "L")]

    def test_single_column(self):
        output = (
            "\nTABLE_NAME                                                          "
            "                                                              \n"
            "------------------------------------------------------------------"
            "------------------------------------------------------------\n"
            "BROOKFL0                                                            "
            "                                                              \n"
            "\n  1 RECORD(S) SELECTED.\n\n"
        )
        desc, rows = _parse_db2_output(output)
        assert desc == ["TABLE_NAME"]
        assert rows == [("BROOKFL0",)]

    def test_no_results(self):
        output = ""
        desc, rows = _parse_db2_output(output)
        assert desc == []
        assert rows == []

    def test_no_separator(self):
        output = "some random text without dashes"
        desc, rows = _parse_db2_output(output)
        assert desc == []
        assert rows == []

    def test_data_with_spaces(self):
        output = (
            "\nNAME                                                             "
            "TABLE_TYPE  SYSTEM_TABLE_NAME \n"
            "----------------------------------------------------------------"
            " ----------- ------------------\n"
            "BROOKFL0                                                          "
            "L           BROOKFL0          \n"
            "\n  1 RECORD(S) SELECTED.\n\n"
        )
        desc, rows = _parse_db2_output(output)
        assert desc == ["NAME", "TABLE_TYPE", "SYSTEM_TABLE_NAME"]
        assert rows == [("BROOKFL0", "L", "BROOKFL0")]

    def test_auto_generated_column_names(self):
        output = (
            "\n00001      00002   \n"
            "---------- --------\n"
            "2026-03-26 16.01.28\n"
            "\n  1 RECORD(S) SELECTED.\n\n"
        )
        desc, rows = _parse_db2_output(output)
        assert desc == ["00001", "00002"]
        assert rows == [("2026-03-26", "16.01.28")]

    def test_skips_blank_lines(self):
        output = "\nA    B   \n---- ----\n\n1    2   \n\n\n  1 RECORD(S) SELECTED.\n\n"
        desc, rows = _parse_db2_output(output)
        assert desc == ["A", "B"]
        assert rows == [("1", "2")]


# --- Error Parsing ---


class TestParseError:
    def test_sqlstate_and_native_error(self):
        output = (
            "\n **** CLI ERROR *****\n"
            "         SQLSTATE: 42704\n"
            "NATIVE ERROR CODE: -204\n"
            "TABLE_XYZ in NONEXISTENT type *FILE not found. \n"
        )
        msg = _parse_error(output)
        assert "SQLSTATE: 42704" in msg
        assert "NATIVE ERROR CODE: -204" in msg
        assert "not found" in msg.lower()

    def test_fallback_to_raw_output(self):
        output = "something unexpected"
        msg = _parse_error(output)
        assert msg == "something unexpected"

    def test_empty_output(self):
        output = ""
        msg = _parse_error(output)
        assert msg == ""


# --- Parameter Substitution ---


class TestQmarkToPositional:
    def test_no_params(self):
        sql, _ = _qmark_to_positional("SELECT 1", [])
        assert sql == "SELECT 1"

    def test_string_param(self):
        sql, _ = _qmark_to_positional("SELECT * FROM t WHERE x = ?", ["hello"])
        assert sql == "SELECT * FROM t WHERE x = 'hello'"

    def test_integer_param(self):
        sql, _ = _qmark_to_positional("SELECT * FROM t WHERE x = ?", [42])
        assert sql == "SELECT * FROM t WHERE x = 42"

    def test_float_param(self):
        sql, _ = _qmark_to_positional("SELECT * FROM t WHERE x = ?", [3.14])
        assert sql == "SELECT * FROM t WHERE x = 3.14"

    def test_none_param(self):
        sql, _ = _qmark_to_positional("SELECT * FROM t WHERE x = ?", [None])
        assert sql == "SELECT * FROM t WHERE x = NULL"

    def test_string_with_single_quote(self):
        sql, _ = _qmark_to_positional("SELECT ?", ["it's"])
        assert sql == "SELECT 'it''s'"

    def test_multiple_params(self):
        sql, _ = _qmark_to_positional(
            "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?",
            ["foo", 42, None],
        )
        assert sql == "SELECT * FROM t WHERE a = 'foo' AND b = 42 AND c = NULL"

    def test_param_count_mismatch(self):
        with pytest.raises(ProgrammingError, match="Expected 2 parameters, got 1"):
            _qmark_to_positional("SELECT ? WHERE x = ?", ["only_one"])

    def test_extra_params(self):
        with pytest.raises(ProgrammingError, match="Expected 1 parameters, got 2"):
            _qmark_to_positional("SELECT ?", ["one", "two"])


# --- Mocked SSH Helpers ---


def _mock_ssh_with_output(output, exit_status=0, error=""):
    """Create a mock SSH client that returns the given output."""
    ssh = MagicMock()
    mock_stdout = MagicMock()
    mock_stdout.read.return_value = output.encode()
    mock_stderr = MagicMock()
    mock_stderr.read.return_value = error.encode()
    mock_stdout.channel.recv_exit_status.return_value = exit_status
    ssh.exec_command.return_value = (MagicMock(), mock_stdout, mock_stderr)
    return ssh


# --- Cursor ---


class TestCursor:
    def test_fetchone(self):
        ssh = _mock_ssh_with_output(
            "\nA   \n----\n1   \n2   \n\n  2 RECORD(S) SELECTED.\n\n"
        )
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        cur.execute("SELECT A FROM t")
        assert cur.fetchone() == ("1",)
        assert cur.fetchone() == ("2",)
        assert cur.fetchone() is None

    def test_fetchall(self):
        ssh = _mock_ssh_with_output(
            "\nA   B  \n---- ---\n1    x  \n2    y  \n\n  2 RECORD(S) SELECTED.\n\n"
        )
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        cur.execute("SELECT A, B FROM t")
        assert cur.fetchall() == [("1", "x"), ("2", "y")]
        assert cur.fetchall() == []

    def test_fetchmany(self):
        ssh = _mock_ssh_with_output(
            "\nA   \n----\n1   \n2   \n3   \n\n  3 RECORD(S) SELECTED.\n\n"
        )
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        cur.execute("SELECT A FROM t")
        assert cur.fetchmany(2) == [("1",), ("2",)]
        assert cur.fetchmany(2) == [("3",)]

    def test_fetchmany_default_arraysize(self):
        ssh = _mock_ssh_with_output(
            "\nA   \n----\n1   \n2   \n\n  2 RECORD(S) SELECTED.\n\n"
        )
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        cur.arraysize = 2
        cur.execute("SELECT A FROM t")
        assert cur.fetchmany() == [("1",), ("2",)]

    def test_iterator(self):
        ssh = _mock_ssh_with_output(
            "\nA   \n----\n1   \n2   \n\n  2 RECORD(S) SELECTED.\n\n"
        )
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        cur.execute("SELECT A FROM t")
        rows = list(cur)
        assert rows == [("1",), ("2",)]

    def test_description(self):
        ssh = _mock_ssh_with_output(
            "\nTABLE_NAME                                                          "
            "TABLE_TYPE \n"
            "-------------------------------------------------------------------"
            " -----------\n"
            "BROOKFL0                                                            "
            "L          \n"
            "\n  1 RECORD(S) SELECTED.\n\n"
        )
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        cur.execute("SELECT TABLE_NAME, TABLE_TYPE FROM t")
        assert cur.description is not None
        assert cur.description[0][0] == "TABLE_NAME"
        assert cur.description[1][0] == "TABLE_TYPE"
        assert len(cur.description[0]) == 7  # PEP 249

    def test_rowcount(self):
        ssh = _mock_ssh_with_output(
            "\nA   \n----\n1   \n2   \n3   \n\n  3 RECORD(S) SELECTED.\n\n"
        )
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        cur.execute("SELECT A FROM t")
        assert cur.rowcount == 3

    def test_execute_returns_self(self):
        ssh = _mock_ssh_with_output("")
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        result = cur.execute("SELECT 1")
        assert result is cur

    def test_execute_with_params(self):
        ssh = _mock_ssh_with_output("\nA   \n----\n1   \n\n  1 RECORD(S) SELECTED.\n\n")
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        cur.execute("SELECT * FROM t WHERE x = ?", ["hello"])
        # Verify the SQL with substituted param was written to remote
        call_args = ssh.exec_command.call_args_list[0][0][0]
        assert "'hello'" in call_args

    def test_execute_closed_connection(self):
        conn = MagicMock()
        conn._closed = True
        cur = Cursor(conn)
        with pytest.raises(InterfaceError, match="Connection is closed"):
            cur.execute("SELECT 1")

    def test_execute_error(self):
        ssh = _mock_ssh_with_output(
            "\n **** CLI ERROR *****\n"
            "         SQLSTATE: 42704\n"
            "NATIVE ERROR CODE: -204\n",
            exit_status=4,
        )
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        with pytest.raises(ProgrammingError, match="SQLSTATE: 42704"):
            cur.execute("SELECT * FROM NOPE")

    def test_close(self):
        conn = MagicMock()
        conn._closed = False
        cur = Cursor(conn)
        cur.execute = MagicMock()
        cur.close()
        assert cur._connection is None
        assert cur.description is None

    def test_executemany(self):
        ssh = _mock_ssh_with_output("\nA   \n----\n1   \n\n  1 RECORD(S) SELECTED.\n\n")
        conn = MagicMock()
        conn._ssh = ssh
        conn._closed = False
        cur = Cursor(conn)
        cur.executemany("SELECT ? FROM t", [["a"], ["b"], ["c"]])
        assert (
            ssh.exec_command.call_count == 9
        )  # 3 queries x 3 calls each (write + exec + cleanup)


# --- Connection ---


class TestConnection:
    @patch("db2ssh.paramiko.SSHClient")
    def test_close(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        conn = connect(host="host", user="user", password="pass")
        assert not conn._closed
        conn.close()
        assert conn._closed
        mock_ssh.close.assert_called_once()

    @patch("db2ssh.paramiko.SSHClient")
    def test_close_idempotent(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        conn = connect(host="host", user="user", password="pass")
        conn.close()
        conn.close()  # should not raise
        assert mock_ssh.close.call_count == 1

    @patch("db2ssh.paramiko.SSHClient")
    def test_context_manager(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        with connect(host="host", user="user", password="pass") as conn:
            cur = conn.cursor()
            assert isinstance(cur, Cursor)
        assert conn._closed

    @patch("db2ssh.paramiko.SSHClient")
    def test_cursor_after_close(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        conn = connect(host="host", user="user", password="pass")
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.cursor()

    @patch("db2ssh.paramiko.SSHClient")
    def test_commit_noop(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        conn = connect(host="host", user="user", password="pass")
        conn.commit()  # should not raise

    @patch("db2ssh.paramiko.SSHClient")
    def test_rollback_raises(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        conn = connect(host="host", user="user", password="pass")
        with pytest.raises(NotSupportedError):
            conn.rollback()


# --- connect() ---


class TestConnect:
    @patch("db2ssh.paramiko.SSHClient")
    def test_explicit_password(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        conn = connect(host="host", user="user", password="pass")
        mock_ssh.connect.assert_called_once()
        assert mock_ssh.connect.call_args[1]["password"] == "pass"

    @patch("db2ssh.paramiko.SSHClient")
    def test_env_var_password(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        with patch.dict(os.environ, {"DB2_PASSWORD": "envpass"}):
            conn = connect(host="host", user="user")
        assert mock_ssh.connect.call_args[1]["password"] == "envpass"

    @patch("db2ssh.paramiko.SSHClient")
    def test_empty_password_treated_as_none(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        conn = connect(host="host", user="user", password="")
        assert mock_ssh.connect.call_args[1]["password"] is None

    @patch("db2ssh.paramiko.SSHClient")
    def test_key_filename(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        conn = connect(host="host", user="user", key_filename="/path/to/key")
        assert mock_ssh.connect.call_args[1]["key_filename"] == "/path/to/key"

    @patch("db2ssh.paramiko.SSHClient")
    def test_no_password_no_key_allowed(self, mock_ssh_cls):
        mock_ssh = MagicMock()
        mock_ssh_cls.return_value = mock_ssh
        # Should not raise — paramiko will try agent/default keys
        conn = connect(host="host", user="user")
        assert mock_ssh.connect.call_args[1]["password"] is None
        assert mock_ssh.connect.call_args[1]["key_filename"] is None

    def test_missing_user_raises(self):
        with pytest.raises(InterfaceError, match="user is required"):
            connect(host="host")

    @patch("db2ssh.paramiko.SSHClient")
    def test_auth_failure_raises_operational_error(self, mock_ssh_cls):
        import paramiko

        mock_ssh = MagicMock()
        mock_ssh.connect.side_effect = paramiko.AuthenticationException("fail")
        mock_ssh_cls.return_value = mock_ssh
        with pytest.raises(OperationalError, match="Authentication failed"):
            connect(host="host", user="user", password="bad")
