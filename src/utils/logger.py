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


import win32serviceutil
import win32service
import win32event
import servicemanager
import sys
import os
import time
from datetime import datetime
import logging
import traceback

# 配置日志
def setup_logging():
    # 修改为本地目录
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'logs')
    # 或者使用绝对路径
    # log_dir = r"D:\logs"
    
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'sync_service_{datetime.now().strftime("%Y%m%d")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

class DailySyncService(win32serviceutil.ServiceFramework):
    _svc_name_ = "DailySyncService"
    _svc_display_name_ = "Daily Data Sync Service"
    _svc_description_ = "每日卫星数据同步服务"
    
    def __init__(self, args):
        try:
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.stop_event = win32event.CreateEvent(None, 0, 0, None)
            self.running = True
            logger.info("服务初始化完成")
        except Exception as e:
            logger.error(f"服务初始化失败: {str(e)}\n{traceback.format_exc()}")
            raise

    def SvcStop(self):
        try:
            logger.info("收到停止服务请求")
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            win32event.SetEvent(self.stop_event)
            self.running = False
        except Exception as e:
            logger.error(f"停止服务失败: {str(e)}\n{traceback.format_exc()}")
            raise

    def SvcDoRun(self):
        try:
            logger.info("服务开始运行")
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)
            
            while self.running:
                logger.info(f"服务正在运行 - {datetime.now()}")
                # 等待60秒或者收到停止信号
                rc = win32event.WaitForSingleObject(self.stop_event, 60 * 1000)
                if rc == win32event.WAIT_OBJECT_0:
                    break
                    
        except Exception as e:
            logger.error(f"服务运行出错: {str(e)}\n{traceback.format_exc()}")
            raise

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