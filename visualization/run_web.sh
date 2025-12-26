#!/bin/bash
# 启动舆情分析系统Web可视化界面

echo "正在启动舆情分析系统Web界面..."
echo "================================================"
echo ""

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# 切换到项目根目录
cd "$PROJECT_ROOT"

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3，请先安装Python"
    exit 1
fi

# 检查依赖
echo "检查依赖包..."
python3 -c "import streamlit" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "未安装Streamlit，正在安装依赖..."
    pip3 install -r requirements.txt
fi

echo ""
echo "启动Web服务器..."
echo "================================================"
echo "访问地址: http://localhost:8501"
echo "按 Ctrl+C 停止服务器"
echo "================================================"
echo ""

# 启动Streamlit应用
streamlit run visualization/app_streamlit.py \
    --server.port=8501 \
    --server.address=localhost \
    --browser.gatherUsageStats=false

