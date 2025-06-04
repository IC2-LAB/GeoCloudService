# src/recommender/run_recommender.py

import oracledb
import os
from .analysis.satellite_data_extractor import SatelliteDataExtractor
from .analysis.order_statistics import OrderAnalyzer
from .dataset.dataset_processor import DatasetProcessor  # 新增
from .dataset.config import DATASET_CONFIG  # 新增
from src.config import config
from src.utils.db.oracle import create_pool

def build_user_datasets(conn):
    """构建用户数据集"""
    print("\n开始构建用户数据集...")
    processor = DatasetProcessor(
        conn=conn,
        output_dir=DATASET_CONFIG['output_dir'],
        neg_pos_ratio=DATASET_CONFIG['neg_pos_ratio']
    )
    processor.process_all_users()
    print(f"用户数据集已保存到目录: {DATASET_CONFIG['output_dir']}")

def main():
    # 1. 建立数据库连接
    try:
        pool = create_pool()
        conn = pool.acquire()
        print("数据库连接成功")
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        return

    # 2. 初始化分析器
    analyzer = OrderAnalyzer(conn)
    extractor = SatelliteDataExtractor(conn)

    # 3. 构建用户数据集（新增）
    build_user_datasets(conn)

    # 4. 获取所有用户
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT F_LOGIN_USER 
            FROM TF_ORDER 
            WHERE F_LOGIN_USER IS NOT NULL
        """)
        users = [row[0] for row in cursor.fetchall()]
        print(f"找到 {len(users)} 个用户")
    except Exception as e:
        print(f"查询用户失败: {str(e)}")
        return
    finally:
        cursor.close()

    # 5. 创建输出目录
    output_dir = "user_satellite_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建输出目录: {output_dir}")

    # 6. 批量处理用户数据
    print("\n开始批量处理用户数据...")
    extractor.batch_process_users(users, output_dir)

    # 7. 生成分析报告
    print("\n生成分析报告...")
    analyzer.print_analysis_report()

    # 8. 关闭数据库连接
    conn.close()
    print("\n处理完成")

if __name__ == "__main__":
    main()