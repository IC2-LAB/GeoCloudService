from cachetools import TTLCache
import threading
import hashlib


class ReadWriteLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._read_ready = threading.Condition(self._lock)
        self._readers = 0

    def acquire_read(self):
        with self._lock:
            self._readers += 1
            self._read_ready.notify()

    def release_read(self):
        with self._lock:
            self._readers -= 1
            self._read_ready.notify()

    def acquire_write(self):
        with self._lock:
            while self._readers > 0:
                self._read_ready.wait()

    def release_write(self):
        with self._lock:
            self._read_ready.notify()

class SimpleCache:
    def __init__(self, maxsize: int = 128, ttl: int = 300):
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self.rw_lock = ReadWriteLock()  # 使用读写锁（如上定义）

    def set(self, key: str, value: any):
        self.rw_lock.acquire_write()
        try:
            self.cache[key] = value
        finally:
            self.rw_lock.release_write()

    def get(self, key: str):
        self.rw_lock.acquire_read()
        try:
            return self.cache.get(key)
        finally:
            self.rw_lock.release_read()

    def delete(self, key: str):
        self.rw_lock.acquire_write()
        try:
            self.cache.pop(key, None)
        finally:
            self.rw_lock.release_write()

    def clear(self):
        self.rw_lock.acquire_write()
        try:
            self.cache.clear()
        finally:
            self.rw_lock.release_write()

class CacheManager:
    def __init__(self, cache: SimpleCache):
        self.cache = cache

    def getCacheKey(self, func_name: str, *args, **kwargs) -> str:
        """生成稳定且唯一的缓存键"""
        args_str = "-".join(map(str, args))
        # 对kwargs的键排序以确保顺序一致
        kwargs_str = "-".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
        key = f"{func_name}-{args_str}-{kwargs_str}"
        # 使用哈希进一步压缩键（可选）
        return hashlib.md5(key.encode()).hexdigest()

    def getData(self, func_name: str, *args, **kwargs) -> any:
        cache_key = self.getCacheKey(func_name, *args, **kwargs)
        return self.cache.get(cache_key)

    def setData(self, func_name: str, data: any, *args, **kwargs):
        cache_key = self.getCacheKey(func_name, *args, **kwargs)
        self.cache.set(cache_key, data)