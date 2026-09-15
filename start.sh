#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="metadata-completion-service"
CONTAINER_NAME="metadata-completion-service"
PORT="18084"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker 未安装或不在 PATH 中" >&2
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  echo "停止并移除已有容器: $CONTAINER_NAME"
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

if ! docker image inspect "$IMAGE_NAME:latest" >/dev/null 2>&1; then
  echo "未找到本地镜像 $IMAGE_NAME:latest，先构建它..."
  docker build -t "$IMAGE_NAME:latest" .
fi

echo "启动容器: $CONTAINER_NAME"
docker run -d \
  --name "$CONTAINER_NAME" \
  -p "$PORT:$PORT" \
  -e PORT="$PORT" \
  --restart unless-stopped \
  "$IMAGE_NAME:latest"

echo "服务已启动，访问地址：http://127.0.0.1:$PORT/docs"
echo "健康检查：http://127.0.0.1:$PORT/healthz"
