@echo off
REM 启动舆情分析系统Web可视化界面 (Windows)

echo 正在启动舆情分析系统Web界面...
echo ================================================
echo.

REM 获取脚本所在目录
set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..

REM 切换到项目根目录
cd /d "%PROJECT_ROOT%"

REM 检查Python环境
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python，请先安装Python
    pause
    exit /b 1
)

REM 检查依赖
echo 检查依赖包...
python -c "import streamlit" >nul 2>&1
if errorlevel 1 (
    echo 未安装Streamlit，正在安装依赖...
    pip install -r requirements.txt
)

echo.
echo 启动Web服务器...
echo ================================================
echo 访问地址: http://localhost:8501
echo 按 Ctrl+C 停止服务器
echo ================================================
echo.

REM 启动Streamlit应用
streamlit run visualization\app_streamlit.py ^
    --server.port=8501 ^
    --server.address=localhost ^
    --browser.gatherUsageStats=false

pause

