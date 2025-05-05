#!/bin/bash

# Đường dẫn thư mục exercises
EX_DIR="./Exercises"

echo "🔍 Kiểm tra và build các Docker image chưa tồn tại..."

for dir in "$EX_DIR"/Exercise-*/; do
    # Lấy số thứ tự bài tập, ví dụ: Exercise-1 => 1
    EX_NUM=$(basename "$dir" | grep -oE '[0-9]+')
    IMAGE_NAME="exercise-$EX_NUM"

    # Kiểm tra image đã tồn tại chưa
    if [[ "$(docker images -q "$IMAGE_NAME" 2> /dev/null)" == "" ]]; then
        echo "🚧 Building image $IMAGE_NAME từ $dir"
        (cd "$dir" && docker build --tag="$IMAGE_NAME" .)
    else
        echo "✅ Image $IMAGE_NAME đã tồn tại, bỏ qua."
    fi
done

echo "🎉 Xong! Các image exercise đã sẵn sàng."
