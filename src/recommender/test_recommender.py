# 示例代码，你可以创建一个新的 test.py 文件
import oracledb
from src.recommender.dataset.dataset_processor import DatasetProcessor
from src.recommender.dataset.user_dataset_builder import UserDatasetBuilder
from src.config import config  # 假设这里有你的数据库配置
from src.utils.db.oracle import create_pool

def test_single_user():
    # 1. 建立数据库连接
    pool = create_pool()
    conn = pool.acquire()
    
    # 2. 设置输出目录
    output_dir = "test_output"  # 你想要的输出目录
    
    # 3. 创建数据集处理器
    processor = DatasetProcessor(conn, output_dir, neg_pos_ratio=5)
    
    # 4. 处理单个用户（例如：'duyinlong_cgs'）
    test_user = 'duyinlong_cgs'  # 你想测试的用户ID
    processor._process_user(test_user)
    
    print(f"数据集已保存到 {output_dir}/{test_user}/")
    conn.close()

if __name__ == "__main__":
    test_single_user()