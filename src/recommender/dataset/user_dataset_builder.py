# src/recommender/dataset/user_dataset_builder.py

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Any
from shapely import wkt
import random

class UserDatasetBuilder:
    def __init__(self, conn, neg_pos_ratio: int = 5):
        """初始化用户数据集构建器
        
        Args:
            conn: Oracle数据库连接
            neg_pos_ratio: 负样本与正样本的比例，默认为5
        """
        self.conn = conn
        self.neg_pos_ratio = neg_pos_ratio
        
    def get_user_dataset(self, user_id: str) -> Tuple[pd.DataFrame, np.ndarray]:
        """为指定用户构建训练数据集
        
        Args:
            user_id: 用户ID
            
        Returns:
            Tuple[pd.DataFrame, np.ndarray]: (特征DataFrame, 标签数组)
        """
        # 1. 获取正样本（购买和加购的数据）
        positive_samples = self._get_positive_samples(user_id)
        pos_count = len(positive_samples)
        
        if pos_count == 0:
            return pd.DataFrame(), np.array([])
            
        # 2. 获取负样本
        negative_samples = self._get_negative_samples(
            user_id, 
            pos_count * self.neg_pos_ratio,
            set(s['data_id'] for s in positive_samples)  # 排除正样本
        )
        
        # 3. 合并样本并处理特征
        all_samples = positive_samples + negative_samples
        features_df = self._process_features(all_samples)
        
        # 4. 构建标签
        labels = np.array([1] * pos_count + [0] * len(negative_samples))
        
        return features_df, labels
    
    def _get_positive_samples(self, user_id: str) -> List[Dict[str, Any]]:
        """获取用户的正样本（购买和加购的数据）"""
        cursor = self.conn.cursor()
        try:
            # 查询用户购买和加购的数据
            cursor.execute("""
                SELECT DISTINCT 
                    d.DATA_ID,
                    d.SATELLITE_ID,
                    d.SENSOR_ID,
                    d.ACQUISITION_TIME,
                    d.CLOUD_COVER,
                    d.RESOLUTION,
                    d.WKT_GEOMETRY,
                    CASE 
                        WHEN o.ORDER_ID IS NOT NULL THEN 1
                        WHEN c.CART_ID IS NOT NULL THEN 1
                    END as INTERACTION_TYPE
                FROM TF_SATELLITE_DATA d
                LEFT JOIN TF_ORDER o ON d.DATA_ID = o.DATA_ID AND o.F_LOGIN_USER = :user_id
                LEFT JOIN TF_CART c ON d.DATA_ID = c.DATA_ID AND c.F_LOGIN_USER = :user_id
                WHERE o.ORDER_ID IS NOT NULL OR c.CART_ID IS NOT NULL
            """, {'user_id': user_id})
            
            columns = [desc[0].lower() for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()
            
    def _get_negative_samples(self, user_id: str, sample_size: int, 
                            exclude_ids: set) -> List[Dict[str, Any]]:
        """随机获取负样本"""
        cursor = self.conn.cursor()
        try:
            # 获取所有可能的负样本
            placeholders = ','.join(f"'{id}'" for id in exclude_ids) if exclude_ids else 'NULL'
            cursor.execute(f"""
                SELECT 
                    DATA_ID,
                    SATELLITE_ID,
                    SENSOR_ID,
                    ACQUISITION_TIME,
                    CLOUD_COVER,
                    RESOLUTION,
                    WKT_GEOMETRY
                FROM TF_SATELLITE_DATA
                WHERE DATA_ID NOT IN ({placeholders})
                ORDER BY DBMS_RANDOM.VALUE
                FETCH FIRST :sample_size ROWS ONLY
            """, {'sample_size': sample_size})
            
            columns = [desc[0].lower() for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()
    
    def _process_features(self, samples: List[Dict[str, Any]]) -> pd.DataFrame:
        """处理特征"""
        processed_data = []
        
        for sample in samples:
            # 1. 处理时间特征
            acquisition_time = sample['acquisition_time']
            time_features = {
                'year': acquisition_time.year,
                'month': acquisition_time.month,
                'day': acquisition_time.day,
                'day_of_week': acquisition_time.weekday(),
                'days_from_now': (datetime.now() - acquisition_time).days
            }
            
            # 2. 处理WKT地理特征
            try:
                geometry = wkt.loads(sample['wkt_geometry'])
                geo_features = {
                    'area': geometry.area,
                    'centroid_lat': geometry.centroid.y,
                    'centroid_lon': geometry.centroid.x,
                }
            except:
                geo_features = {
                    'area': 0,
                    'centroid_lat': 0,
                    'centroid_lon': 0,
                }
            
            # 3. 其他特征
            other_features = {
                'data_id': sample['data_id'],
                'satellite_id': sample['satellite_id'],
                'sensor_id': sample['sensor_id'],
                'cloud_cover': float(sample['cloud_cover']),
                'resolution': float(sample['resolution']),
            }
            
            # 合并所有特征
            processed_data.append({
                **time_features,
                **geo_features,
                **other_features
            })
        
        # 转换为DataFrame
        df = pd.DataFrame(processed_data)
        
        # 特征标准化/归一化
        numeric_columns = ['cloud_cover', 'resolution', 'area']
        df[numeric_columns] = (df[numeric_columns] - df[numeric_columns].mean()) / df[numeric_columns].std()
        
        return df