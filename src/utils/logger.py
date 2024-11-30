# import logging
# import datetime
# logger = logging.getLogger("main")
# stream_handler = logging.StreamHandler()

# time_f_str = "%Y-%m-%d_%H-%M-%S"
# file_handler = logging.FileHandler(f"{datetime.datetime.now().strftime(time_f_str)}.log")

# stream_handler.setLevel(logging.INFO)
# file_handler.setLevel(logging.DEBUG)

# log_fmt = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
# stream_handler.setFormatter(log_fmt)
# file_handler.setFormatter(log_fmt)

# logger.addHandler(stream_handler)
# logger.addHandler(file_handler)

# logger.setLevel(logging.DEBUG)

# def info(message):
#     logger.info(message)

# def debug(message):
#     logger.debug(message)

# def error(message):
#     logger.error(message)

import logging
import os
from datetime import datetime

def setup_logging():
    """配置日志系统"""
    # 创建日志目录
    log_dir = r'C:\bupt_server\DZY\GeoCloudService\log'
    os.makedirs(log_dir, exist_ok=True)
    
    # 创建日志文件名（按日期）
    log_file = os.path.join(log_dir, f'sync_{datetime.now().strftime("%Y%m%d")}.log')
    
    # 创建logger实例
    logger = logging.getLogger('sync')
    logger.setLevel(logging.DEBUG)  # 设置最低日志级别
    
    # 防止重复添加handler
    if not logger.handlers:
        # 创建文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 创建日志格式
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 设置处理器格式
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器到logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# 创建全局logger实例
logger = setup_logging()

# 便捷的日志函数
def debug(message):
    """记录调试信息"""
    logger.debug(message)

def info(message):
    """记录一般信息"""
    logger.info(message)

def warning(message):
    """记录警告信息"""
    logger.warning(message)

def error(message):
    """记录错误信息"""
    logger.error(message)

def critical(message):
    """记录严重错误信息"""
    logger.critical(message)

# 添加异常跟踪记录功能
def log_exception(e, message="发生异常"):
    """记录异常信息和堆栈跟踪"""
    import traceback
    error_msg = f"{message}: {str(e)}\n{traceback.format_exc()}"
    logger.error(error_msg)

if __name__ == '__main__':
    try:
        if len(sys.argv) == 1:
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(DailySyncService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
            win32serviceutil.HandleCommandLine(DailySyncService)
    except Exception as e:
        logger.error(f"服务主程序错误: {str(e)}\n{traceback.format_exc()}")
        raise