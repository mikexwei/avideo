#!/bin/bash

# 检查输入参数
if [ -z "$1" ]; then
    echo "用法: $0 input.mp4"
    exit 1
fi

INPUT_FILE="$1"
FILENAME="${INPUT_FILE%.*}"
OUTPUT_FILE="${FILENAME}_converted.mp4"

echo "🔍 正在分析视频流格式: $INPUT_FILE ..."

# 使用 ffprobe 提取视频编码格式 (剔除多余输出，只保留编码名称)
VIDEO_CODEC=$(ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 "$INPUT_FILE")

echo "ℹ️ 检测到视频编码为: $VIDEO_CODEC"

# 根据不同的编码，应用不同的极速封装策略
if [ "$VIDEO_CODEC" = "hevc" ]; then
    echo "🚀 正在极速封装 (HEVC/H.265 模式) ..."
    # HEVC 必须加 hvc1 标签以兼容 Apple 设备
    ffmpeg -i "$INPUT_FILE" -c copy -tag:v hvc1 -movflags +faststart "$OUTPUT_FILE"

elif [ "$VIDEO_CODEC" = "h264" ]; then
    echo "🚀 正在极速封装 (H.264 模式) ..."
    # H.264 本身就兼容，绝对不能加 hvc1 标签
    ffmpeg -i "$INPUT_FILE" -c copy -movflags +faststart "$OUTPUT_FILE"

else
    echo "⚠️ 警告：检测到格式为 $VIDEO_CODEC，可能无法在 Apple 设备上仅靠 copy 播放。"
    echo "⏳ 尝试常规封装..."
    ffmpeg -i "$INPUT_FILE" -c copy -movflags +faststart "$OUTPUT_FILE"
fi

# 检查上一条 ffmpeg 命令的执行结果
if [ $? -eq 0 ]; then
    echo "✅ 转换成功！(耗时极短)"
else
    echo "❌ 转换失败，可能原编码不支持直接 copy 或文件已损坏。"
fi