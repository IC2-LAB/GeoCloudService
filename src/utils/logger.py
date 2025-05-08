import logging
import datetime
import queue
import time
import threading
from logging.handlers import QueueHandler, QueueListener

logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)

# 设置控制台日志处理器
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
log_fmt = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
stream_handler.setFormatter(log_fmt)
logger.addHandler(stream_handler)

# 定义时间格式和文件名
time_f_str = "%Y-%m-%d_%H-%M-%S"
file_name = f"Y:/logs/{datetime.datetime.now().strftime(time_f_str)}.log"

# 创建队列用于异步处理日志
log_queue = queue.Queue(-1)  # 使用无界队列避免日志丢失


class RetryingFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=True, retry_interval=5):
        super().__init__(filename, mode, encoding, delay)
        self.retry_interval = retry_interval
        self._lock = threading.Lock()
        self._pending_logs = []
        self._retry_thread = None
        self._running = True
        # self._local_backup_file = 'local_log_backup.log'
        self._start_retry_thread()

    def _start_retry_thread(self):
        """启动一个后台线程来处理重试"""
        self._retry_thread = threading.Thread(target=self._flush_loop)
        self._retry_thread.daemon = True
        self._retry_thread.start()

    def emit(self, record):
        msg = self.format(record)
        with self._lock:
            self._pending_logs.append(msg)
        if not self._retry_thread.is_alive():
            self._start_retry_thread()

    def _reopen_stream(self):
        """尝试重新打开文件流"""
        if self.stream is not None:
            self.stream.close()
            self.stream = None
        try:
            self.stream = self._open()
        except Exception as e:
            # logger.error(f"无法重新打开日志文件流: {e}")
            self.stream = None

    def _flush_loop(self):
        while self._running:
            try:
                if self.stream is None:
                    self._reopen_stream()
                if self.stream is None:
                    # logger.error("无法打开日志文件流，等待下一次重试...")
                    time.sleep(self.retry_interval)
                    continue

                with self._lock:
                    while self._pending_logs:
                        log_msg = self._pending_logs[0]
                        try:
                            self.stream.write(log_msg + '\n')
                            self.stream.flush()
                            self._pending_logs.pop(0)
                        except Exception as e:
                            self._reopen_stream()  # 如果写入失败，则尝试重新打开文件流
                            break

                time.sleep(self.retry_interval)
            except Exception as e:
                logger.error(f"处理日志时发生错误，等待下一次重试。错误：{e}")
                time.sleep(self.retry_interval)


    def close(self):
        self._running = False
        if self.stream:
            self.stream.close()
        super().close()


# 创建自定义文件处理器并配置
file_handler = RetryingFileHandler(file_name, encoding='utf-8', retry_interval=1)  # 设置半小时的重试间隔
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(log_fmt)

# 创建队列监听器并启动
listener = QueueListener(log_queue, file_handler, respect_handler_level=True)
listener.start()

# 创建队列处理器并添加到主日志器
queue_handler = QueueHandler(log_queue)
queue_handler.setLevel(logging.DEBUG)
logger.addHandler(queue_handler)

def info(message):
    logger.info(message)

def debug(message):
    logger.debug(message)

def error(message):
    logger.error(message)

# 确保程序退出时停止监听器
import atexit
atexit.register(listener.stop)
atexit.register(file_handler.close)

