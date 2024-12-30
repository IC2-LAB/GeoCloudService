import logging
import os
from datetime import datetime

# 创建 logs 目录（如果不存在）
if not os.path.exists('logs'):
    os.makedirs('logs')

# 获取当前日期作为文件名的一部分
current_date = datetime.now().strftime('%Y%m%d')

# 创建日志格式化器
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 创建文件处理器
file_handler = logging.FileHandler(f'logs/import_{current_date}.log', encoding='utf-8')
file_handler.setFormatter(formatter)

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# 获取 logger 实例
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 添加处理器
logger.addHandler(file_handler)
logger.addHandler(console_handler)