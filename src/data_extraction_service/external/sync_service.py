import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import os
from pathlib import Path
import time
from datetime import datetime
import threading

class DataSyncService(win32serviceutil.ServiceFramework):
    _svc_name_ = "DataSyncService"
    _svc_display_name_ = "Data Synchronization Service"
    _svc_description_ = "Service for synchronizing data between internal and external networks"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.running = True

    def SvcStop(self):
        """
        服务停止时的处理
        """
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        self.running = False

    def SvcDoRun(self):
        """
        服务运行时的主要逻辑
        """
        try:
            # 初始化日志
            self.initialize_logging()
            
            # 切换到项目根目录
            project_dir = Path(__file__).parent.parent.parent.parent
            os.chdir(str(project_dir))
            
            # 启动同步服务
            self.start_sync_service()
            
        except Exception as e:
            servicemanager.LogErrorMsg(f"Service failed: {str(e)}")
            
    def initialize_logging(self):
        """
        初始化日志配置
        """
        try:
            from src.utils.logger import setup_logger
            self.logger = setup_logger()
            self.logger.info("Data Sync Service started")
        except Exception as e:
            servicemanager.LogErrorMsg(f"Failed to initialize logging: {str(e)}")
            
    def start_sync_service(self):
        """
        启动同步服务
        """
        try:
            from src.data_extraction_service.external.main import sync_data
            
            # 创建健康检查线程
            health_check_thread = threading.Thread(target=self.health_check)
            health_check_thread.daemon = True
            health_check_thread.start()
            
            # 执行同步任务
            sync_data('daily')
            
        except Exception as e:
            self.logger.error(f"Failed to start sync service: {str(e)}")
            
    def health_check(self):
        """
        执行健康检查，定期写入状态文件
        """
        health_file = os.path.join("logs", "service_health.txt")
        while self.running:
            try:
                with open(health_file, 'w') as f:
                    f.write(f"Service alive at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(60)  # 每分钟更新一次
            except Exception as e:
                self.logger.error(f"Health check failed: {str(e)}")

def main():
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(DataSyncService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(DataSyncService)

if __name__ == '__main__':
    main()