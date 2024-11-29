@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

echo 正在准备安装服务...

:: 创建日志目录
mkdir "..\logs" 2>nul

:: 设置 Python 路径
set "PYTHON_PATH=%~dp0..\.venv\Scripts\python.exe"
set "SERVICE_SCRIPT=%~dp0..\src\data_extraction_service\external\daily_sync_service.py"

echo Python路径: %PYTHON_PATH%
echo 服务脚本: %SERVICE_SCRIPT%

:: 检查文件是否存在
if not exist "%PYTHON_PATH%" (
    echo 错误: Python解释器不存在
    goto :error
)

if not exist "%SERVICE_SCRIPT%" (
    echo 错误: 服务脚本不存在
    goto :error
)

echo 正在删除旧服务...
net stop DailySyncService 2>nul
"%PYTHON_PATH%" "%SERVICE_SCRIPT%" remove 2>nul

echo 正在安装新服务...
"%PYTHON_PATH%" "%SERVICE_SCRIPT%" install
if errorlevel 1 goto :error

echo 正在启动服务...
timeout /t 2 /nobreak >nul
net start DailySyncService
if errorlevel 1 goto :error

echo 检查服务状态...
sc query DailySyncService

goto :end

:error
echo 安装过程中出现错误！
echo 请检查日志文件: ..\logs\service_test.log
type ..\logs\service_test.log

:end
pause