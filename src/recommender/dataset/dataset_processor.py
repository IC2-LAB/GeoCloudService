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
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 获取所有用户
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT F_LOGIN_USER 
                FROM TF_ORDER 
                WHERE F_LOGIN_USER IS NOT NULL
            """)
            users = [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()
            
        # 处理每个用户的数据集
        for user_id in users:
            self._process_user(user_id)
            
    def _process_user(self, user_id: str) -> None:
        """处理单个用户的数据集"""
        # 构建用户数据集
        features_df, labels = self.dataset_builder.get_user_dataset(user_id)
        
        if len(features_df) == 0:
            return
            
        # 保存数据集
        user_dir = os.path.join(self.output_dir, user_id)
        os.makedirs(user_dir, exist_ok=True)
        
        features_df.to_csv(os.path.join(user_dir, 'features.csv'), index=False)
        np.save(os.path.join(user_dir, 'labels.npy'), labels)