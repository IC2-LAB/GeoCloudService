import oracledb
from typing import Dict, List, Any
import numpy as np

class ItemFeatureExtractor:
    def __init__(self, conn):
        """初始化卫星数据特征提取器
        
        Args:
            conn: Oracle数据库连接
        """
        self.conn = conn
        self.cursor = conn.cursor()
        
    def get_satellite_basic_features(self) -> Dict[str, Dict[str, Any]]:
        """获取卫星数据基本特征
        
        Returns:
            Dict[数据ID, 特征字典]
        """
        # 获取卫星数据基本信息
        self.cursor.execute("""
            SELECT 
                DATA_ID,
                SATELLITE_TYPE,
                RESOLUTION,
                CLOUD_COVER,
                IMAGING_TIME,
                REGION_CODE
            FROM SATELLITE_DATA_INFO
        """)
        
        satellite_features = {}
        for row in self.cursor.fetchall():
            data_id, sat_type, resolution, cloud_cover, imaging_time, region = row
            satellite_features[data_id] = {
                "satellite_type": sat_type,
                "resolution": float(resolution) if resolution else 0.0,
                "cloud_cover": float(cloud_cover) if cloud_cover else 0.0,
                "imaging_time": imaging_time,
                "region_code": region
            }
            
        return satellite_features
    
    def get_order_statistics(self) -> Dict[str, Dict[str, float]]:
        """获取卫星数据订购统计信息
        
        Returns:
            Dict[数据ID, 统计特征字典]
        """
        # 获取每个数据的订购次数和最近订购时间
        self.cursor.execute("""
            SELECT 
                DATA_ID,
                COUNT(*) as order_count,
                MAX(ORDER_TIME) as last_order_time,
                COUNT(DISTINCT USER_ID) as unique_users
            FROM TF_ORDER
            GROUP BY DATA_ID
        """)
        
        order_stats = {}
        for row in self.cursor.fetchall():
            data_id, order_count, last_order, unique_users = row
            order_stats[data_id] = {
                "order_count": int(order_count),
                "last_order_time": last_order,
                "unique_users": int(unique_users)
            }
            
        return order_stats
    
    def get_region_popularity(self) -> Dict[str, float]:
        """获取地区热度统计
        
        Returns:
            Dict[地区代码, 热度分数]
        """
        self.cursor.execute("""
            SELECT 
                s.REGION_CODE,
                COUNT(*) as order_count
            FROM TF_ORDER o
            JOIN SATELLITE_DATA_INFO s ON o.DATA_ID = s.DATA_ID
            GROUP BY s.REGION_CODE
        """)
        
        region_counts = {}
        total_orders = 0
        for row in self.cursor.fetchall():
            region, count = row
            region_counts[region] = int(count)
            total_orders += count
            
        # 计算地区热度得分
        region_popularity = {
            region: count / total_orders
            for region, count in region_counts.items()
        }
        
        return region_popularity
    
    def extract_item_features(self) -> Dict[str, Dict[str, Any]]:
        """提取所有卫星数据特征
        
        Returns:
            Dict[数据ID, 特征字典]
        """
        basic_features = self.get_satellite_basic_features()
        order_stats = self.get_order_statistics()
        region_popularity = self.get_region_popularity()
        
        item_features = {}
        for data_id, basic in basic_features.items():
            stats = order_stats.get(data_id, {
                "order_count": 0,
                "unique_users": 0,
                "last_order_time": None
            })
            
            item_features[data_id] = {
                **basic,  # 基本特征
                "order_count": stats["order_count"],
                "unique_users": stats["unique_users"],
                "region_popularity": region_popularity.get(basic["region_code"], 0.0),
                "popularity_score": (stats["order_count"] + stats["unique_users"]) / 2  # 简单的热度计算
            }
            
        return item_features 