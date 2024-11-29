@echo off
chcp 65001 > nul

echo 正在停止服务...
net stop DailySyncService

echo 正在删除服务...
"%~dp0..\.venv\Scripts\python.exe" "%~dp0..\src\data_extraction_service\external\daily_sync_service.py" remove

echo 完成！
pause