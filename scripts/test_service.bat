@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

echo 测试服务安装和运行
echo ====================

:: 设置路径
set "PYTHON_PATH=%~dp0..\.venv\Scripts\python.exe"
set "SERVICE_SCRIPT=%~dp0..\src\data_extraction_service\external\daily_sync_service.py"

:: 1. 停止并删除现有服务
echo 步骤 1: 清理现有服务...
net stop DailySyncService 2>nul
"%PYTHON_PATH%" "%SERVICE_SCRIPT%" remove 2>nul
timeout /t 2 /nobreak >nul

:: 2. 安装服务
echo 步骤 2: 安装服务...
"%PYTHON_PATH%" "%SERVICE_SCRIPT%" install
if errorlevel 1 (
    echo 服务安装失败
    goto :error
)
timeout /t 2 /nobreak >nul

:: 3. 启动服务
echo 步骤 3: 启动服务...
"%PYTHON_PATH%" "%SERVICE_SCRIPT%" start
if errorlevel 1 (
    echo 服务启动失败
    goto :error
)
timeout /t 2 /nobreak >nul

:: 4. 检查服务状态
echo 步骤 4: 检查服务状态...
sc query DailySyncService

echo.
echo 请检查Windows事件查看器中的应用程序日志
echo 以查看详细的服务运行信息
echo.
echo 按任意键打开事件查看器...
pause >nul
eventvwr.msc

goto :end

:error
echo.
echo 发生错误！请检查Windows事件查看器中的应用程序日志
echo 按任意键打开事件查看器...
pause >nul
eventvwr.msc

:end
endlocal