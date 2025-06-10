# src/recommender/dataset/user_dataset_builder.py

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Any
from shapely import wkt
import random
from .config import DATASET_CONFIG 

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
        
        # 2. 获取负样本
        negative_samples = self._get_negative_samples(user_id, positive_samples)
        
        # 3. 添加标签
        for sample in positive_samples:
            sample['label'] = 1
        for sample in negative_samples:
            sample['label'] = 0
        
        # 4. 合并样本并创建DataFrame
        all_samples = positive_samples + negative_samples
        df = pd.DataFrame(all_samples)
        
        # 5. 分离特征和标签
        labels = df['label'].values
        features_df = df.drop('label', axis=1)
        
        return features_df, labels
    
    def _get_positive_samples(self, user_id: str) -> List[Dict[str, Any]]:
        """获取用户的正样本数据"""
        cursor = self.conn.cursor()
        try:
            # 1. 获取用户的订单数据
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
            print(f"\n找到 {len(order_data)} 条订单数据")
            
            positive_samples = []
            
            # 2. 根据卫星类型查询对应的元数据表
            for data_name, satellite, sensor in order_data:
                try:
                    meta_table = self._get_meta_table(satellite)
                    if not meta_table:
                        print(f"未找到卫星 {satellite} 对应的元数据表")
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
                        # 直接将查询结果转换为字典
                        column_names = [
                            "F_DATANAME", "F_PRODUCTID", "F_DATAID", "F_SATELLITEID",
                            "F_SENSORID", "F_CLOUDPERCENT", "F_RECEIVETIME", "F_DATASIZE",
                            "F_TOPLEFTLATITUDE", "F_TOPLEFTLONGITUDE",
                            "F_BOTTOMRIGHTLATITUDE", "F_BOTTOMRIGHTLONGITUDE",
                            "F_PRODUCTLEVEL"
                        ]
                        sample = dict(zip(column_names, row))
                        positive_samples.append(sample)
                        
                except Exception as e:
                    print(f"处理数据 {data_name} 失败: {str(e)}")
                    continue
                        
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
                sat_id = sample['F_SATELLITEID']
                satellite_counts[sat_id] = satellite_counts.get(sat_id, 0) + 1
            
            # 3. 按比例获取负样本
            negative_samples = []
            for sat_id, count in satellite_counts.items():
                needed_count = count * self.neg_pos_ratio
                meta_table = self._get_meta_table(sat_id)
                if not meta_table:
                    continue

                # 修改：使用 NOT EXISTS 而不是 NOT IN
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
                        FROM {meta_table} m
                        WHERE NOT EXISTS (
                            SELECT 1 
                            FROM TF_ORDER o
                            JOIN TF_ORDERDATA od ON o.F_ID = od.F_ORDERID
                            WHERE o.F_LOGIN_USER = :user_id
                            AND od.F_DATANAME = m.F_DATANAME
                        )
                        AND ROWNUM <= :needed_count
                        ORDER BY DBMS_RANDOM.VALUE
                    )
                    SELECT * FROM RANDOM_SAMPLES
                """, {
                    'user_id': user_id,
                    'needed_count': needed_count
                })
                
                # 直接将查询结果转换为字典列表
                column_names = [
                    "F_DATANAME", "F_PRODUCTID", "F_DATAID", "F_SATELLITEID",
                    "F_SENSORID", "F_CLOUDPERCENT", "F_RECEIVETIME", "F_DATASIZE",
                    "F_TOPLEFTLATITUDE", "F_TOPLEFTLONGITUDE",
                    "F_BOTTOMRIGHTLATITUDE", "F_BOTTOMRIGHTLONGITUDE",
                    "F_PRODUCTLEVEL"
                ]
                
                for row in cursor.fetchall():
                    sample = dict(zip(column_names, row))
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
            "GF1BCD": "JGF_GXFW.TB_META_GF1BCD",
            "GF1BCD ": "JGF_GXFW.TB_META_GF1BCD",
            "ZY3-1": "TB_META_ZY301",
            "ZY3-2": "TB_META_ZY302",
            "ZY303": "TB_META_ZY303",
            "ZY301": "TB_META_ZY301",
            "ZY302": "TB_META_ZY302",
            "ZY303": "TB_META_ZY303",
            "GF5": "TB_META_GF5",
            "GF6": "TB_META_GF6",
            "GF7": "TB_META_GF7",
            "ZY1E": "TB_META_ZY1E",
            "ZY1F": "TB_META_ZY1F",
            "CB04A":"TB_META_CB04A",

        }
        return satellite_mapping.get(satellite)
    
    def _process_features(self, samples: List[Dict[str, Any]]) -> pd.DataFrame:
        """处理特征"""
        processed_data = []
        
        for sample in samples:
            try:
                # 1. 处理时间特征
                receive_time = sample['receive_time']
                time_features = {
                    'year': receive_time.year,
                    'month': receive_time.month,
                    'day': receive_time.day,
                    'day_of_week': receive_time.weekday(),
                    'days_from_now': (datetime.now() - receive_time).days
                }
                
                # 2. 处理空间特征
                bbox = sample['bbox']
                spatial_features = {
                    'top_left_lat': bbox['top_left'][0],
                    'top_left_lon': bbox['top_left'][1],
                    'bottom_right_lat': bbox['bottom_right'][0],
                    'bottom_right_lon': bbox['bottom_right'][1]
                }
                
                # 3. 其他特征
                other_features = {
                    'data_id': sample['data_id'],
                    'satellite_id': sample['satellite_id'],
                    'sensor_id': sample['sensor_id'],
                    'cloud_cover': float(sample['cloud_cover']),
                    'data_size': float(sample.get('data_size', 0)),
                    'product_level': sample.get('product_level', '')
                }
                
                # 合并所有特征
                processed_data.append({
                    **time_features,
                    **spatial_features,
                    **other_features
                })
            except Exception as e:
                print(f"Warning: Error processing sample: {str(e)}")
                print(f"Sample data: {sample}")
                continue
        
        # 创建DataFrame并确保所有列都存在
        df = pd.DataFrame(processed_data)
        
        # 确保所有配置的特征列都存在
        for col in DATASET_CONFIG['feature_columns']:
            if col not in df.columns:
                df[col] = 0  # 或者其他适当的默认值
                
        # 只返回配置中指定的列
        return df[DATASET_CONFIG['feature_columns']]
    
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
    
    def _safe_float(value):
        """安全地将值转换为浮点数"""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0