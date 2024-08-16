#!/bin/bash

# -----------------------------------------------------------------------------
# Type of I/O pattern. Accepted values are:
# read, write, randread, randwrite, rw(readwrite), randrw,
# trim, randtrim, trimwrite, randtrimwrite
# -----------------------------------------------------------------------------

# 配置参数
mode=read
block_size=4k
file_size=50G
run_time=10
file_dir="/opt/meituan/fio_test"

iodepth=1
direct=1 # skip page cache
ioengine=libaio # sync/libaio/mmap

block_align=4096
offset_align=4096

# 任务线程数列表
jobs=(8 16 32 64 128 256 512 1024 2048 4096)

# 函数：清理文件目录
clean_directory() {
    if [ -n "$(ls -A "$file_dir")" ]; then
        rm -f "$file_dir"/*
    fi
}

# 函数：运行 FIO 测试
run_fio_test() {
    local job=$1
    local file_name="${file_dir}/test_io_file"
    local job_name="perf_${mode}_${job}thread"
    local report_name="${block_size}_${job}j_${mode}"

    echo "## job $job_name running"

    cmd="fio -filename=$file_name -thread -numjobs=$job -iodepth=$iodepth -rw=$mode -direct=$direct -ioengine=$ioengine -bs=$block_size -size=$file_size -group_reporting -name=$job_name --output-format=normal --runtime=$run_time --time_based --blockalign=$block_align --offset_align=$offset_align --output=$report_name"
    echo $cmd && eval $cmd

    echo "## done"
    sleep 5
}

# 主程序
echo "FIO testing mode: $mode, file_dir: $file_dir"

for job in "${jobs[@]}"; do
    clean_directory
    run_fio_test $job
done

# 清理测试文件
echo "## test done, removing test files"
clean_directory
