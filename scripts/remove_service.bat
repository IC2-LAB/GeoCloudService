@echo off
chcp 65001 > nul
echo 正在移除服务...
python main.py remove
echo 服务已移除
pause