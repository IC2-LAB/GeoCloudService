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
        """为指定用户构建训练数据集"""
        # 1. 获取正样本
        positive_samples = self._get_positive_samples(user_id)
        if not positive_samples:
            return pd.DataFrame(), np.array([])
        
        # 2. 获取负样本（3倍正样本，保持卫星分布）
        negative_samples = self._get_negative_samples(user_id, positive_samples)
        
        # 3. 合并样本并处理特征
        all_samples = positive_samples + negative_samples
        features_df = self._process_features(all_samples)
        
        # 4. 构建标签
        labels = np.array([1] * len(positive_samples) + [0] * len(negative_samples))
        
        return features_df, labels
    
    def _get_positive_samples(self, user_id: str) -> List[Dict[str, Any]]:
        """获取用户的正样本数据"""
        cursor = self.conn.cursor()
        try:
            # 1. 首先获取用户的订单数据
            cursor.execute("""
                SELECT 
                    od.F_DATANAME,
                    od.F_SATELITE,
                    od.F_SENSOR
                FROM TF_ORDER o
                JOIN TF_ORDERDATA od ON o.F_ID = od.F_ORDERID
                WHERE o.F_LOGIN_USER = :user_id
            """, {'user_id': user_id})
            
            order_data = cursor.fetchall()
            positive_samples = []
            
            # 2. 根据卫星类型查询对应的元数据表
            for data_name, satellite, sensor in order_data:
                # 获取对应的卫星元数据表名
                meta_table = self._get_meta_table(satellite)
                if not meta_table:
                    continue
                    
                # 3. 从元数据表获取详细特征
                cursor.execute(f"""
                    SELECT 
                        F_DATANAME,
                        F_PRODUCTID,
                        F_DATAID,
                        F_SATELLITEID,
                        F_SENSORID,
                        F_CLOUDPERCENT,
                        F_RECEIVETIME,
                        F_DATASIZE,
                        F_TOPLEFTLATITUDE,
                        F_TOPLEFTLONGITUDE,
                        F_BOTTOMRIGHTLATITUDE,
                        F_BOTTOMRIGHTLONGITUDE,
                        F_PRODUCTLEVEL
                    FROM {meta_table}
                    WHERE F_DATANAME = :dataname
                """, {'dataname': data_name})
                
                row = cursor.fetchone()
                if row:
                    sample = {
                        'data_name': row[0],
                        'product_id': row[1],
                        'data_id': row[2],
                        'satellite_id': row[3],
                        'sensor_id': row[4],
                        'cloud_cover': float(row[5]) if row[5] is not None else 0,
                        'receive_time': row[6],
                        'data_size': float(row[7]) if row[7] is not None else 0,
                        'bbox': {
                            'top_left': (float(row[8]) if row[8] is not None else 0,
                                      float(row[9]) if row[9] is not None else 0),
                            'bottom_right': (float(row[10]) if row[10] is not None else 0,
                                          float(row[11]) if row[11] is not None else 0)
                        },
                        'product_level': row[12]
                    }
                    positive_samples.append(sample)
                    
            return positive_samples
        finally:
            cursor.close()
            
    def _get_negative_samples(self, user_id: str, positive_samples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """获取负样本数据"""
        cursor = self.conn.cursor()
        try:
            # 1. 获取用户已有的订单数据名称
            cursor.execute("""
                SELECT od.F_DATANAME
                FROM TF_ORDER o
                JOIN TF_ORDERDATA od ON o.F_ID = od.F_ORDERID
                WHERE o.F_LOGIN_USER = :user_id
            """, {'user_id': user_id})
            
            existing_data = {row[0] for row in cursor.fetchall()}
            
            # 2. 计算正样本中各卫星类型的分布
            satellite_counts = {}
            for sample in positive_samples:
                sat_id = sample['satellite_id']
                satellite_counts[sat_id] = satellite_counts.get(sat_id, 0) + 1
                
            # 3. 按比例获取负样本
            negative_samples = []
            for sat_id, count in satellite_counts.items():
                # 获取3倍的负样本
                needed_count = count * 3
                
                # 找到对应的元数据表
                meta_table = self._get_meta_table(sat_id)
                if not meta_table:
                    continue
                    
                # 随机获取负样本
                cursor.execute(f"""
                    WITH RANDOM_SAMPLES AS (
                        SELECT 
                            F_DATANAME,
                            F_PRODUCTID,
                            F_DATAID,
                            F_SATELLITEID,
                            F_SENSORID,
                            F_CLOUDPERCENT,
                            F_RECEIVETIME,
                            F_DATASIZE,
                            F_TOPLEFTLATITUDE,
                            F_TOPLEFTLONGITUDE,
                            F_BOTTOMRIGHTLATITUDE,
                            F_BOTTOMRIGHTLONGITUDE,
                            F_PRODUCTLEVEL
                        FROM {meta_table}
                        WHERE F_DATANAME NOT IN ({','.join([f"'{d}'" for d in existing_data])})
                        AND ROWNUM <= :needed_count
                        ORDER BY DBMS_RANDOM.VALUE
                    )
                    SELECT * FROM RANDOM_SAMPLES
                """, {'needed_count': needed_count})
                
                for row in cursor.fetchall():
                    sample = {
                        'data_name': row[0],
                        'product_id': row[1],
                        'data_id': row[2],
                        'satellite_id': row[3],
                        'sensor_id': row[4],
                        'cloud_cover': float(row[5]) if row[5] is not None else 0,
                        'receive_time': row[6],
                        'data_size': float(row[7]) if row[7] is not None else 0,
                        'bbox': {
                            'top_left': (float(row[8]) if row[8] is not None else 0,
                                      float(row[9]) if row[9] is not None else 0),
                            'bottom_right': (float(row[10]) if row[10] is not None else 0,
                                          float(row[11]) if row[11] is not None else 0)
                        },
                        'product_level': row[12]
                    }
                    negative_samples.append(sample)
                    
            return negative_samples
        finally:
            cursor.close()

    def _get_meta_table(self, satellite: str) -> str:
        """获取卫星对应的元数据表名"""
        satellite_mapping = {
                "GF1": "TB_META_GF1",
                "GF2": "TB_META_GF2",
                "ZY02C": "TB_META_ZY02C",
                "GF1B": "TB_META_GF1B",
                "GF1C": "TB_META_GF1C",
                "GF1D": "TB_META_GF1D",
                "GF1BCD": "TB_META_GF1BCD",
                "ZY3-1": "TB_META_ZY301",
                "ZY3-2": "TB_META_ZY302",
                "ZY3-3": "TB_META_ZY303",
                "GF5": "TB_META_GF5",
                "GF6": "TB_META_GF6",
                "GF7": "TB_META_GF7",
        }
        return satellite_mapping.get(satellite)
    
    def _process_features(self, samples: List[Dict[str, Any]]) -> pd.DataFrame:
        """处理特征"""
        processed_data = []
        
        for sample in samples:
            # 1. 处理时间特征
            receive_time = sample['receive_time']
            time_features = {
                'year': receive_time.year,
                'month': receive_time.month,
                'day': receive_time.day,
                'day_of_week': receive_time.weekday(),
                'days_from_now': (datetime.now() - receive_time).days
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
    
    def _batch_query(self, cursor, base_query: str, id_list: list, batch_size: int = 900) -> list:
        """批量处理大量ID的查询
        
        Args:
            cursor: 数据库游标
            base_query: 基础SQL查询语句（包含 IN 占位符 {})
            id_list: ID列表
            batch_size: 每批处理的ID数量
            
        Returns:
            所有批次查询结果的组合
        """
        results = []
        for i in range(0, len(id_list), batch_size):
            batch = id_list[i:i + batch_size]
            placeholders = ','.join([f"'{id_}'" for id_ in batch])
            query = base_query.format(placeholders)
            cursor.execute(query)
            results.extend(cursor.fetchall())
        return results