@echo off
REM Windows 快速安装脚本 - 舆情分析系统
REM 自动处理 PySpark 兼容性问题

echo ========================================
echo 舆情分析系统 - Windows 快速安装
echo ========================================
echo.

REM 检查 Python 版本
echo [1/5] 检查 Python 版本...
python --version 2>nul
if errorlevel 1 (
    echo [错误] 未找到 Python，请先安装 Python 3.11 或 3.12
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo Python 版本: %PYTHON_VERSION%

REM 检查 Python 3.13 兼容性警告
echo %PYTHON_VERSION% | findstr /C:"3.13" >nul
if not errorlevel 1 (
    echo.
    echo [警告] 检测到 Python 3.13
    echo PySpark 在 Python 3.13 + Windows 上存在兼容性问题
    echo 建议使用 Python 3.11 或 3.12
    echo.
    echo 按任意键继续（将使用兼容性补丁）...
    pause >nul
)

REM 检查 Java
echo.
echo [2/5] 检查 Java...
java -version 2>nul
if errorlevel 1 (
    echo [警告] 未找到 Java，PySpark 需要 Java 8 或 11
    echo 下载地址: https://adoptium.net/
    echo.
    echo 按任意键继续...
    pause >nul
)

REM 创建虚拟环境
echo.
echo [3/5] 创建虚拟环境...
if exist venv (
    echo 虚拟环境已存在，跳过创建
) else (
    python -m venv venv
    if errorlevel 1 (
        echo [错误] 创建虚拟环境失败
        pause
        exit /b 1
    )
    echo 虚拟环境创建成功
)

REM 激活虚拟环境
echo.
echo [4/5] 激活虚拟环境...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo [错误] 激活虚拟环境失败
    pause
    exit /b 1
)

REM 升级 pip
echo.
echo [4.5/5] 升级 pip 和核心工具...
python -m pip install --upgrade pip
python -m pip install --upgrade setuptools packaging

REM 安装依赖
echo.
echo [5/5] 安装 Python 依赖包...
pip install -r requirements.txt
if errorlevel 1 (
    echo [错误] 安装依赖失败
    pause
    exit /b 1
)

REM 运行兼容性检查
echo.
echo ========================================
echo 运行 PySpark 兼容性检查...
echo ========================================
python pyspark_windows_compat.py

REM 完成
echo.
echo ========================================
echo 安装完成！
echo ========================================
echo.
echo 下一步：
echo 1. 确保虚拟环境已激活（命令行前应显示 (venv)）
echo 2. 运行快速测试: python scripts\quick_start.py
echo 3. 或运行完整应用: python src\main\python\main\app.py --generate -n 100
echo.
echo 如遇到问题，请查看：
echo - ENVIRONMENT_SETUP.md
echo - PYSPARK_WINDOWS_FIX.md
echo.
echo 按任意键退出...
pause >nul

