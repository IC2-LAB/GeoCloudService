@echo off
chcp 65001 > nul
echo 正在安装服务...
python main.py install --startup auto
echo 服务安装完成
pause