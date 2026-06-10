import json
import os
import sys

from airflow.providers.ssh.hooks.ssh import SSHHook


def main():
    username = os.getenv("SSH_USERNAME")
    password = os.getenv("SSH_PASSWORD")
    assert username, "SSH_USERNAME environment variable is not set"
    assert password, "SSH_PASSWORD environment variable is not set"

    hook = SSHHook(
        remote_host="localhost",
        username=username,
        password=password,
        port=2222,
        conn_timeout=30,
    )

    client = hook.get_conn()
    try:
        stdin, stdout, stderr = client.exec_command(
            "python3 /opt/scripts/etl.py --date 2024-01-15"
        )

        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8").strip()
        err = stderr.read().decode("utf-8").strip()

        print(f"Exit code : {exit_code}")
        print(f"Stdout    : {out}")
        print(f"Stderr    : {err}")

        if exit_code == 0 and out:
            result = json.loads(out)
            print(f"\nParsed XCom result: {result}")
        else:
            print("\nSomething went wrong")
            sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
