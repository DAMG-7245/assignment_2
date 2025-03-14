# 使用官方 Python 3.9 精简版镜像
FROM python:3.9-slim

# 切换工作目录
WORKDIR /app

# 将依赖文件复制到容器
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用主文件 (例如 main.py) 到容器
COPY main.py .

# 预设容器监听 8080 端口（可在运行时通过 PORT 环境变量覆盖）
ENV PORT=8080

# 对外暴露 8080 端口
EXPOSE 8080

# 以 uvicorn 启动 FastAPI 服务，监听 $PORT
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT"]
