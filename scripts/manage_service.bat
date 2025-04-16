@echo off
chcp 65001 > nul

if "%1"=="" goto usage

echo 正在执行服务命令: %1
python main.py %1
goto end

:usage
echo 用法: %0 [start^|stop^|restart^|status]
echo 示例: %0 start

:end
pause