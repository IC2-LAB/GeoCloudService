# src/recommender/dataset/dataset_processor.py

import os
import pandas as pd
import numpy as np
from typing import Dict, List
from .user_dataset_builder import UserDatasetBuilder

class DatasetProcessor:
    def __init__(self, conn, output_dir: str, neg_pos_ratio: int = 5):
        """初始化数据集处理器
        
        Args:
            conn: Oracle数据库连接
            output_dir: 数据集输出目录
            neg_pos_ratio: 负样本与正样本的比例
        """
        self.conn = conn
        self.output_dir = output_dir
        self.dataset_builder = UserDatasetBuilder(conn, neg_pos_ratio)
        
    def process_all_users(self) -> None:
        """处理所有用户的数据集"""
        os.makedirs(self.output_dir, exist_ok=True)
        
        cursor = self.conn.cursor()
        try:
            # 获取有效订单的用户
            cursor.execute("""
                SELECT DISTINCT o.F_LOGIN_USER
                FROM TF_ORDER o
                JOIN TF_ORDERDATA od ON o.F_ID = od.F_ORDERID
                WHERE o.F_LOGIN_USER IS NOT NULL
            """)
            users = [row[0] for row in cursor.fetchall()]
            print(f"找到 {len(users)} 个有效用户")
            
            # 处理每个用户的数据集
            for i, user_id in enumerate(users, 1):
                try:
                    print(f"\n处理用户 {i}/{len(users)}: {user_id}")
                    self._process_user(user_id)
                except Exception as e:
                    print(f"处理用户 {user_id} 失败: {str(e)}")
                    continue
                    
        finally:
            cursor.close()
            
    def _process_user(self, user_id: str) -> None:
        """处理单个用户的数据集"""
        # 构建用户数据集
        features_df, labels = self.dataset_builder.get_user_dataset(user_id)
        
        if len(features_df) == 0:
            return
            
        # 保存数据集
        user_dir = os.path.join(self.output_dir, user_id)
        os.makedirs(user_dir, exist_ok=True)
        
        # 将标签列添加回特征DataFrame
        output_df = features_df.copy()
        output_df['label'] = labels
        
        # 保存完整的数据集（包含特征和标签）
        output_df.to_csv(os.path.join(user_dir, 'features.csv'), index=False)