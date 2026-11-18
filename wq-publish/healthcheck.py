"""容器健康检查：HTTP 探活首页。

首页返回 2xx/3xx 视为健康（退出 0），任何异常或非成功状态码均退出 1，
且不向标准错误打印堆栈，保持容器日志整洁。端口与 Dockerfile 的 EXPOSE/CMD 对齐。
"""
import sys
import urllib.request

URL = "http://127.0.0.1:5000/"

try:
    with urllib.request.urlopen(URL, timeout=4) as resp:
        sys.exit(0 if 200 <= resp.status < 400 else 1)
except Exception:
    sys.exit(1)
