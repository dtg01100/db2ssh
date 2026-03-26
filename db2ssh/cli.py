"""db2ssh CLI — Run SQL queries on IBM i Db2 via SSH."""

import paramiko
import getpass
import argparse
import os
import sys
import uuid

from db2ssh import _parse_db2_output, _parse_error


def parse_args():
    parser = argparse.ArgumentParser(
        description="Query IBM i Db2 over SSH.",
        prog="db2ssh",
    )
    parser.add_argument("--host", required=True, help="IBM i hostname or IP")
    parser.add_argument("--user", required=True, help="IBM i username")
    parser.add_argument("--query", help="SQL query to execute")
    parser.add_argument("--file", help="SQL file to execute (local path)")
    parser.add_argument(
        "--password",
        help="IBM i password (or set DB2_PASSWORD env var)",
    )
    parser.add_argument(
        "--key-file",
        help="Path to private key file for key-based auth",
    )
    parser.add_argument("--output", help="Write output to file instead of stdout")
    args = parser.parse_args()
    if not args.query and not args.file:
        parser.error("Either --query or --file is required")
    if args.query and args.file:
        parser.error("Specify only one of --query or --file")
    return args


def get_auth(args):
    """Resolve authentication. Returns (password, key_filename)."""
    password = args.password or os.environ.get("DB2_PASSWORD")
    if password == "":
        password = None
    key_file = args.key_file

    if not password and not key_file:
        try:
            pw = getpass.getpass(
                f"Password for {args.user}@{args.host} (enter to try key auth): "
            )
            password = pw if pw else None
        except Exception:
            pass

    return password, key_file


def get_sql(args):
    if args.query:
        return args.query
    with open(args.file) as f:
        return f.read()


def run_query(ssh, sql):
    remote_sql = f"/tmp/.db2ssh_{uuid.uuid4().hex}.sql"

    escaped_sql = sql.replace("\\", "\\\\").replace("'", "'\\''")
    ssh.exec_command(f"printf '%s\\n' '{escaped_sql}' > {remote_sql}")

    try:
        cmd = f'qsh -c "db2 -f {remote_sql}"'
        stdin, stdout, stderr = ssh.exec_command(cmd)
        output = stdout.read().decode()
        error = stderr.read().decode()
        exit_status = stdout.channel.recv_exit_status()
    finally:
        ssh.exec_command(f"rm -f {remote_sql}")

    return output, error, exit_status


def main():
    args = parse_args()
    password, key_file = get_auth(args)
    sql = get_sql(args)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(
            args.host,
            username=args.user,
            password=password,
            key_filename=key_file,
            timeout=10,
        )
    except paramiko.AuthenticationException:
        print(f"Authentication failed for {args.user}@{args.host}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"SSH connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        output, error, exit_status = run_query(ssh, sql)

        if exit_status != 0:
            print(output, file=sys.stderr, end="")
            if error:
                print(error, file=sys.stderr, end="")
            sys.exit(exit_status)

        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"Output written to {args.output}")
        else:
            print(output, end="")
    finally:
        ssh.close()


if __name__ == "__main__":
    main()
