import os
import time
import threading
from minio import Minio
from minio.error import S3Error

# -------------------配置区-------------------
endpoint = "139.9.202.246:9000"
access_key = "R4u6Y661PIcieNdkodhJ"
secret_key = "KtaN3V3tnZJHtn0TtAfG6G5cmgPpfsPn9jWuYPlK"
bucket_name = "dcuav"
file_path = r"./20250716135200/20210817002_0004.JPG"
task_id = "xxx20250716"
object_name = f"{task_id}/CUT_1_nn.las"
# -------------------------------------------

# 初始化 MinIO 客户端
minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

# 创建桶
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# 单位格式化工具
def format_size(bytes_size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.2f} PB"

def format_speed(bytes_per_s):
    kbps = bytes_per_s / 1024
    if kbps < 1024:
        return f"{kbps:.2f} KB/s"
    else:
        return f"{kbps / 1024:.2f} MB/s"

# 上传监控类
class UploadMonitor:
    def __init__(self, file_path, chunk_size=5 * 1024 * 1024):
        self.file = open(file_path, 'rb')
        self.chunk_size = chunk_size
        self.total_size = os.path.getsize(file_path)
        self.uploaded = 0
        self.start_time = time.time()
        self._lock = threading.Lock()
        self._stop = False

    def read(self, size=-1):
        if size == -1:
            size = self.chunk_size
        data = self.file.read(size)
        with self._lock:
            self.uploaded += len(data)
        return data

    def __len__(self):
        return self.total_size

    def close(self):
        self._stop = True
        self.file.close()

    def is_done(self):
        return self.uploaded >= self.total_size or self._stop

# 后台打印实时速度
def print_progress(monitor):
    while not monitor.is_done():
        with monitor._lock:
            elapsed = time.time() - monitor.start_time
            speed = monitor.uploaded / elapsed if elapsed > 0 else 0
            percent = monitor.uploaded / monitor.total_size * 100
            print(f"⏳ 已上传: {percent:.2f}% | 实时速度: {format_speed(speed)}")
        time.sleep(1)

# 上传执行
try:
    monitor = UploadMonitor(file_path)

    # 启动进度打印线程
    progress_thread = threading.Thread(target=print_progress, args=(monitor,))
    progress_thread.start()

    # 上传
    minio_client.put_object(
        bucket_name,
        object_name,
        data=monitor,
        length=len(monitor),
        part_size=5 * 1024 * 1024
    )

    monitor.close()
    progress_thread.join()

    total_time = time.time() - monitor.start_time
    avg_speed = monitor.uploaded / total_time

    print("\n✅ 上传完成")
    print(f"📦 文件大小: {format_size(monitor.total_size)}")
    print(f"⏱️ 总上传时间: {total_time:.2f} 秒")
    print(f"📊 平均上传速度: {format_speed(avg_speed)}")
except S3Error as e:
    print(f"❌ 上传失败: {e}")
