@echo off
chcp 65001 >nul
echo ========================================
echo Python+Spark 热词分析系统
echo ========================================
echo.

REM 检查 Python 是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到 Python，请先安装 Python 3.7+
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/3] 检查 Python 环境...
python --version

echo.
echo [2/3] 检查依赖包...
python -c "import pandas, jieba, snownlp, matplotlib, wordcloud" >nul 2>&1
if errorlevel 1 (
    echo [警告] 缺少必要的依赖包
    echo 正在安装依赖包...
    echo.
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
    if errorlevel 1 (
        echo [错误] 依赖安装失败，请手动运行: pip install -r requirements.txt
        pause
        exit /b 1
    )
) else (
    echo [成功] 依赖包已安装
)

echo.
echo [3/3] 启动程序...
echo.
python main.py

if errorlevel 1 (
    echo.
    echo [错误] 程序运行失败
    pause
)

