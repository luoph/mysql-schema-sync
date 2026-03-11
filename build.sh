#!/bin/bash

# 定义版本和项目名称
PROJECT_NAME="db-schema-sync"
TARGET_DIR="target"
DIST_DIR="${TARGET_DIR}/dist"

# 清理并创建目录
rm -rf "${TARGET_DIR}"
mkdir -p "${DIST_DIR}"

# 编译平台列表
PLATFORMS=("linux/amd64" "linux/arm64" "darwin/amd64" "darwin/arm64" "windows/amd64")

for PLATFORM in "${PLATFORMS[@]}"; do
    OS=${PLATFORM%/*}
    ARCH=${PLATFORM#*/}

    OUTPUT_NAME="${PROJECT_NAME}"
    if [ "$OS" = "windows" ]; then
        OUTPUT_NAME="${OUTPUT_NAME}.exe"
    fi

    echo "Building for ${OS}/${ARCH}..."

    # 执行编译
    CGO_ENABLED=0 GOOS=${OS} GOARCH=${ARCH} go build -a \
        -ldflags '-extldflags="-static" -s -w' \
        -o "${TARGET_DIR}/${OS}_${ARCH}/${OUTPUT_NAME}" .

    # 准备打包目录
    PACKAGE_DIR="${DIST_DIR}/${PROJECT_NAME}_${OS}_${ARCH}"
    mkdir -p "${PACKAGE_DIR}"
    cp "${TARGET_DIR}/${OS}_${ARCH}/${OUTPUT_NAME}" "${PACKAGE_DIR}/"
    cp config.json LICENSE README.md "${PACKAGE_DIR}/"

    # 打包压缩
    cd "${DIST_DIR}"
    if [ "$OS" = "windows" ]; then
        zip -q -r "${PROJECT_NAME}_${OS}_${ARCH}.zip" "${PROJECT_NAME}_${OS}_${ARCH}"
    else
        tar -czf "${PROJECT_NAME}_${OS}_${ARCH}.tar.gz" "${PROJECT_NAME}_${OS}_${ARCH}"
    fi
    cd - > /dev/null
done

echo "Build complete! Check the ${DIST_DIR} directory."
