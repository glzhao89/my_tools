#!/bin/bash

# 检查是否提供了路径参数
if [ -z "$1" ]; then
    echo "请提供一个路径参数。"
    echo "用法: $0 <路径>"
    exit 1
fi

# 获取路径参数
base_path=$1

# 定义要统计的目录
directories=("doris2doris" "doris2hive" "common" "dsn" "plus")

# 遍历每个目录并使用 cloc 统计代码量
for dir in "${directories[@]}"; do
    full_path="$base_path/$dir"
    if [ -d "$full_path" ]; then
        echo "统计目录: $full_path"
        cloc "$full_path"
    else
        echo "目录 $full_path 不存在"
    fi
done
