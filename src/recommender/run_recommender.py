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
    try:
        # 1. 建立数据库连接
        pool = create_pool()
        conn = pool.acquire()
        print("数据库连接成功")

        # 2. 构建用户数据集
        build_user_datasets(conn)

        # 3. 生成分析报告
        analyzer = OrderAnalyzer(conn)
        print("\n生成分析报告...")
        analyzer.print_analysis_report()

    except Exception as e:
        print(f"处理失败: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()
        print("\n处理完成")

if __name__ == "__main__":
    main()