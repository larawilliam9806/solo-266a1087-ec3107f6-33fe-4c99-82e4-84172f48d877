# 水质批次修订发布台 —— 容器镜像
# 仅依赖 Flask（Web 框架）与 waitress（生产级 WSGI 服务器），全部为纯 Python 包。
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    WQ_DATA_DIR=/data \
    WQ_DOWNSTREAM_DIR=/downstream

WORKDIR /app

# 先单独复制依赖清单，利用 Docker 层缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 再复制应用代码
COPY wq_publish ./wq_publish
COPY wsgi.py healthcheck.py ./

# 数据登记（SQLite、密钥、隔离区）与下游发布目录均以卷挂载持久化，
# 镜像内预先创建并交给非 root 用户运行
RUN mkdir -p /data /downstream \
    && useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /data /downstream /app
USER appuser

EXPOSE 5000

# 通过 --call 调用 wsgi:create_app 工厂，由环境变量决定数据/下游目录
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-I", "healthcheck.py"]

CMD ["waitress-serve", "--call", "--host=0.0.0.0", "--port=5000", "wsgi:create_app"]
