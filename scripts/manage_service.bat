@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

:: 检查管理员权限
net session >nul 2>&1
if %errorLevel% == 0 (
    goto :admin
) else (
    echo 请求管理员权限...
    powershell -Command "Start-Process '%~dpnx0' -Verb RunAs"
    exit /b
)

:admin
:: 切换到项目根目录
cd /d %~dp0
cd ..

:: 设置 Python 虚拟环境路径
set "VENV_PYTHON=%CD%\.venv\Scripts\python.exe"

:menu
cls
echo 数据同步服务管理工具
echo ====================
echo 1. 安装服务
echo 2. 启动服务
echo 3. 停止服务
echo 4. 重启服务
echo 5. 删除服务
echo 6. 查看状态
echo 7. 查看日志
echo 8. 退出

:: 设置 Python 脚本路径
set "SERVICE_SCRIPT=%CD%\src\data_extraction_service\external\daily_sync_service.py"

set /p choice=请选择操作 (1-8): 

if "%choice%"=="1" (
    echo 正在安装服务...
    "%VENV_PYTHON%" "%SERVICE_SCRIPT%" install
    timeout /t 2 >nul
) else if "%choice%"=="2" (
    echo 正在启动服务...
    "%VENV_PYTHON%" "%SERVICE_SCRIPT%" start
    timeout /t 2 >nul
) else if "%choice%"=="3" (
    echo 正在停止服务...
    "%VENV_PYTHON%" "%SERVICE_SCRIPT%" stop
    timeout /t 2 >nul
) else if "%choice%"=="4" (
    echo 正在重启服务...
    "%VENV_PYTHON%" "%SERVICE_SCRIPT%" restart
    timeout /t 2 >nul
) else if "%choice%"=="5" (
    echo 正在删除服务...
    "%VENV_PYTHON%" "%SERVICE_SCRIPT%" remove
    timeout /t 2 >nul
) else if "%choice%"=="6" (
    echo 正在查询服务状态...
    sc query DailySyncService
    timeout /t 5 >nul
) else if "%choice%"=="7" (
    echo 正在打开日志...
    if exist "logs\sync_service_%date:~0,4%%date:~5,2%%date:~8,2%.log" (
        start notepad "logs\sync_service_%date:~0,4%%date:~5,2%%date:~8,2%.log"
    ) else (
        echo 日志文件不存在
        timeout /t 2 >nul
    )
) else if "%choice%"=="8" (
    exit /b
) else (
    echo 无效的选择！
    timeout /t 2 >nul
)

:: 返回菜单
goto :menu