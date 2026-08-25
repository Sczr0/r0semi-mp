#!/usr/bin/env python3
"""SSH 到生产服务器执行命令（调试用）。用法：python tools/sshrun.py "<命令>" """
import sys, paramiko

HOST, PORT, USER, PW = "160.202.238.171", 47300, "root", "8m2x8CAuUdFa"

def run(cmd, timeout=30):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(HOST, port=PORT, username=USER, password=PW, timeout=15)
    stdin, stdout, stderr = c.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", "replace")
    err = stderr.read().decode("utf-8", "replace")
    c.close()
    return out, err

if __name__ == "__main__":
    cmd = sys.argv[1]
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    out, err = run(cmd, timeout)
    if out:
        print(out)
    if err:
        print("STDERR:", err)
