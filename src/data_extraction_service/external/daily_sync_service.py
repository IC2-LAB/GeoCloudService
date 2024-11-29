import win32serviceutil
import win32service
import win32event
import win32evtlogutil
import servicemanager
import sys
import os
import time
from datetime import datetime

class DailySyncService(win32serviceutil.ServiceFramework):
    _svc_name_ = "DailySyncService"
    _svc_display_name_ = "Daily Data Sync Service"
    _svc_description_ = "Daily Data Synchronization Service"

    def __init__(self, args):
        try:
            # 初始化服务
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.stop_event = win32event.CreateEvent(None, 0, 0, None)
            self.running = True
            
            # 记录事件
            win32evtlogutil.ReportEvent(
                self._svc_name_,
                1,
                eventType=win32evtlog.EVENTLOG_INFORMATION_TYPE,
                strings=["Service initialized successfully"]
            )
        except Exception as e:
            # 记录错误
            win32evtlogutil.ReportEvent(
                self._svc_name_,
                1,
                eventType=win32evtlog.EVENTLOG_ERROR_TYPE,
                strings=[f"Service initialization failed: {str(e)}"]
            )
            raise

    def SvcStop(self):
        try:
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            win32event.SetEvent(self.stop_event)
            self.running = False
        except Exception as e:
            win32evtlogutil.ReportEvent(
                self._svc_name_,
                1,
                eventType=win32evtlog.EVENTLOG_ERROR_TYPE,
                strings=[f"Service stop failed: {str(e)}"]
            )
            raise

    def SvcDoRun(self):
        try:
            win32evtlogutil.ReportEvent(
                self._svc_name_,
                1,
                eventType=win32evtlog.EVENTLOG_INFORMATION_TYPE,
                strings=["Service starting"]
            )
            
            while self.running:
                # 每10秒记录一次运行状态
                win32evtlogutil.ReportEvent(
                    self._svc_name_,
                    1,
                    eventType=win32evtlog.EVENTLOG_INFORMATION_TYPE,
                    strings=[f"Service running at {datetime.now()}"]
                )
                
                # 等待10秒或者收到停止信号
                rc = win32event.WaitForSingleObject(self.stop_event, 10 * 1000)
                if rc == win32event.WAIT_OBJECT_0:
                    break
                    
        except Exception as e:
            win32evtlogutil.ReportEvent(
                self._svc_name_,
                1,
                eventType=win32evtlog.EVENTLOG_ERROR_TYPE,
                strings=[f"Service run failed: {str(e)}"]
            )
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
        win32evtlogutil.ReportEvent(
            "DailySyncService",
            1,
            eventType=win32evtlog.EVENTLOG_ERROR_TYPE,
            strings=[f"Service main failed: {str(e)}"]
        )
        raise