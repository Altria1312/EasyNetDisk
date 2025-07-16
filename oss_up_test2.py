from minio import Minio, InvalidResponseError, S3Error
from minio import Minio
import datetime
import os

class Bucket:

    def __init__(self, minio_address, minio_admin, minio_password):
        # 通过ip 账号 密码 连接minio server
        # Http连接 将secure设置为False
        self.minioClient = Minio(endpoint=minio_address,
                                 access_key=minio_admin,
                                 secret_key=minio_password,
                                 secure=False)

    def create_one_bucket(self, bucket_name):
        # 创建桶(调用make_bucket api来创建一个桶)
        """
        桶命名规则：小写字母，句点，连字符和数字 允许使用 长度至少3个字符
        使用大写字母、下划线等会报错
        """
        try:
            # bucket_exists：检查桶是否存在
            if self.minioClient.bucket_exists(bucket_name=bucket_name):
                print("该存储桶已经存在")
            else:
                self.minioClient.make_bucket(bucket_name=bucket_name)
                print(f"{bucket_name}桶创建成功")
        except InvalidResponseError as err:
            print(err)

    def remove_one_bucket(self, bucket_name):
        # 删除桶(调用remove_bucket api来创建一个存储桶)
        try:
            if self.minioClient.bucket_exists(bucket_name=bucket_name):
                self.minioClient.remove_bucket(bucket_name)
                print("删除存储桶成功")
            else:
                print("该存储桶不存在")
        except InvalidResponseError as err:
            print(err)

    def upload_file_to_bucket(self, bucket_name, file_name, file_path):
        """
        将文件上传到bucket
        :param bucket_name: minio桶名称
        :param file_name: 存放到minio桶中的文件名字(相当于对文件进行了重命名，可以与原文件名不同)
                            file_name处可以创建新的目录(文件夹) 例如 /example/file_name
                            相当于在该桶中新建了一个example文件夹 并把文件放在其中
        :param file_path: 本地文件的路径
        """
        # 桶是否存在 不存在则新建
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if not check_bucket:
            self.minioClient.make_bucket(bucket_name)

        try:
            self.minioClient.fput_object(bucket_name=bucket_name,
                                         object_name=file_name,
                                         file_path=file_path)
        except FileNotFoundError as err:
            print('upload_failed: ' + str(err))
        except S3Error as err:
            print("upload_failed:", err)

    def download_file_from_bucket(self, bucket_name, minio_file_path, download_file_path):
        """
        从bucket下载文件
        :param bucket_name: minio桶名称
        :param minio_file_path: 存放在minio桶中文件名字
                            file_name处可以包含目录(文件夹) 例如 /example/file_name
        :param download_file_path: 文件获取后存放的路径
        """
        # 桶是否存在
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if check_bucket:
            try:
                self.minioClient.fget_object(bucket_name=bucket_name,
                                             object_name=minio_file_path,
                                             file_path=download_file_path)
            except FileNotFoundError as err:
                print('download_failed: ' + str(err))
            except S3Error as err:
                print("download_failed:", err)

    def remove_object(self, bucket_name, object_name):
        """
        从bucket删除文件
        :param bucket_name: minio桶名称
        :param object_name: 存放在minio桶中的文件名字
                            object_name处可以包含目录(文件夹) 例如 /example/file_name
        """
        # 桶是否存在
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if check_bucket:
            try:
                self.minioClient.remove_object(bucket_name=bucket_name,
                                               object_name=object_name)
            except FileNotFoundError as err:
                print('upload_failed: ' + str(err))
            except S3Error as err:
                print("upload_failed:", err)

    # 获取所有的桶
    def get_all_bucket(self):
        buckets = self.minioClient.list_buckets()
        ret = []
        for _ in buckets:
            ret.append(_.name)
        return ret

    # 获取一个桶中的所有一级目录和文件
    def get_list_objects_from_bucket(self, bucket_name):
        # 桶是否存在
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if check_bucket:
            # 获取到该桶中的所有目录和文件
            objects = self.minioClient.list_objects(bucket_name=bucket_name)
            ret = []
            for _ in objects:
                ret.append(_.object_name)
            return ret

    # 获取桶里某个目录下的所有目录和文件
    def get_list_objects_from_bucket_dir(self, bucket_name, dir_name):
        # 桶是否存在
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if check_bucket:
            # 获取到bucket_name桶中的dir_name下的所有目录和文件
            # prefix 获取的文件路径需包含该前缀
            objects = self.minioClient.list_objects(bucket_name=bucket_name,
                                                    prefix=dir_name,
                                                    recursive=True)
            ret = []
            for _ in objects:
                ret.append(_.object_name)
            return ret

# MinIO 服务器地址和身份验证信息
endpoint = "218.65.206.83:9000"
access_key = "vPfLEI3cVlCcSjxtFcpm"
secret_key = "kkgqkmDNgCfA5Ee4CkmJCmp90JEhkGIWt4BIMAQn"

bucket = Bucket(minio_address=endpoint,
                minio_admin=access_key,
                minio_password=secret_key)
# 初始化 MinIO 客户端
minio_client = Minio(
endpoint,
access_key=access_key,
secret_key=secret_key,
secure=False # 设置为 False 以使用 HTTP，若使用 HTTPS 则改为 True 这里只需要设置为False
)
# 目标桶和文件路径设置
bucket_name = "dcuav" # 固定桶名称
folder_path = './20250716135200' # 替换为实际的文件夹路径
suffix = ".JPG"
file_names = os.listdir(folder_path)
jpg_files = [file for file in file_names if file.endswith(suffix)]


count = 0
images_list = []
for file_name in file_names:
    count = count + 1
for k in range(count):
    images_list.append(0)
start=datetime.datetime.now()
print("运行开始时间：", start)
for image_up in file_names:
    if image_up != 0:
        #print(image_up)
        up_name = "./20250716135200/" + image_up
        up_oss = "20250716135200/"+image_up
        print(up_name)
        print(up_oss)
        ret = bucket.upload_file_to_bucket('dcuav', up_oss, up_name)
        print(f"文件 '{image_up}' 成功上传至 '{bucket_name}/{up_oss}'")
end=datetime.datetime.now()
print("运行结束时间：", end)
print('程序运行时间为: %s Seconds'%(end-start))


# file_path = "G:/20250506163745/"+jpg_files 
# # 本地文件路径
# task_id = "20250506163745" # 飞行任务 ID
# object_name = f"{task_id}/jpg_files" # 存储名称 = 任务 ID + 文件名
# # 检查桶是否存在，不存在则创建
# if not minio_client.bucket_exists(bucket_name):
#     minio_client.make_bucket(bucket_name)
# # 上传文件
# try:    
# #需要计算的代码块
#     start=datetime.datetime.now()
#     minio_client.fput_object(bucket_name, object_name, file_path)
#     end=datetime.datetime.now()
#     print('程序运行时间为: %s Seconds'%(end-start))
#     print(f"文件 '{file_path}' 成功上传至 '{bucket_name}/{object_name}'")
# except S3Error as e:
#     print(f"上传失败: {e}")
