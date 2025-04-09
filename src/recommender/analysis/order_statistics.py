import oracledb
from typing import Dict, Any
import pandas as pd
from datetime import datetime

class OrderAnalyzer:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()
        
    def get_basic_stats(self) -> Dict[str, Any]:
        """获取基本统计信息"""
        # 1. 总订单数和用户数
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_orders,
                COUNT(DISTINCT F_USERID) as total_users,
                COUNT(DISTINCT F_SATELLITE) as total_satellites
            FROM TF_ORDER
        """)
        total_stats = dict(zip(['total_orders', 'total_users', 'total_satellites'], 
                             self.cursor.fetchone()))
        
        # 2. 用户订单分布
        self.cursor.execute("""
            SELECT 
                F_USERID,
                COUNT(*) as order_count,
                MIN(F_CREATTIME) as first_order,
                MAX(F_CREATTIME) as last_order
            FROM TF_ORDER
            GROUP BY F_USERID
            ORDER BY order_count DESC
        """)
        user_stats = self.cursor.fetchall()
        
        # 3. 卫星类型分布
        self.cursor.execute("""
            SELECT 
                F_SATELLITE,
                COUNT(*) as usage_count,
                COUNT(DISTINCT F_USERID) as user_count
            FROM TF_ORDER
            WHERE F_SATELLITE IS NOT NULL
            GROUP BY F_SATELLITE
            ORDER BY usage_count DESC
        """)
        satellite_stats = self.cursor.fetchall()
        
        # 4. 时间分布
        self.cursor.execute("""
            SELECT 
                EXTRACT(YEAR FROM F_CREATTIME) as year,
                EXTRACT(MONTH FROM F_CREATTIME) as month,
                COUNT(*) as order_count,
                COUNT(DISTINCT F_USERID) as active_users
            FROM TF_ORDER
            GROUP BY 
                EXTRACT(YEAR FROM F_CREATTIME),
                EXTRACT(MONTH FROM F_CREATTIME)
            ORDER BY year, month
        """)
        time_stats = self.cursor.fetchall()
        
        return {
            "basic_stats": total_stats,
            "user_stats": user_stats,
            "satellite_stats": satellite_stats,
            "time_stats": time_stats
        }
    
    def get_user_preferences(self) -> Dict[str, Any]:
        """分析用户偏好"""
        # 1. 用户卫星偏好
        self.cursor.execute("""
            SELECT 
                F_USERID,
                F_SATELLITE,
                COUNT(*) as usage_count
            FROM TF_ORDER
            WHERE F_SATELLITE IS NOT NULL
            GROUP BY F_USERID, F_SATELLITE
            ORDER BY F_USERID, usage_count DESC
        """)
        satellite_preferences = self.cursor.fetchall()
        
        # 2. 用户地区偏好
        self.cursor.execute("""
            SELECT 
                F_USERID,
                F_PROVINCESPACE,
                COUNT(*) as region_count
            FROM TF_ORDER
            WHERE F_PROVINCESPACE IS NOT NULL
            GROUP BY F_USERID, F_PROVINCESPACE
            ORDER BY F_USERID, region_count DESC
        """)
        region_preferences = self.cursor.fetchall()
        
        return {
            "satellite_preferences": satellite_preferences,
            "region_preferences": region_preferences
        }
    
    def get_data_quality_stats(self) -> Dict[str, Any]:
        """分析数据质量统计"""
        # 1. 云量分布
        self.cursor.execute("""
            SELECT 
                CASE 
                    WHEN F_CLOUDAMOUNT <= 20 THEN '0-20%'
                    WHEN F_CLOUDAMOUNT <= 40 THEN '21-40%'
                    WHEN F_CLOUDAMOUNT <= 60 THEN '41-60%'
                    WHEN F_CLOUDAMOUNT <= 80 THEN '61-80%'
                    ELSE '81-100%'
                END as cloud_range,
                COUNT(*) as count
            FROM TF_ORDER
            WHERE F_CLOUDAMOUNT IS NOT NULL
            GROUP BY CASE 
                WHEN F_CLOUDAMOUNT <= 20 THEN '0-20%'
                WHEN F_CLOUDAMOUNT <= 40 THEN '21-40%'
                WHEN F_CLOUDAMOUNT <= 60 THEN '41-60%'
                WHEN F_CLOUDAMOUNT <= 80 THEN '61-80%'
                ELSE '81-100%'
            END
            ORDER BY cloud_range
        """)
        cloud_stats = self.cursor.fetchall()
        
        # 2. 数据大小分布
        self.cursor.execute("""
            SELECT 
                CASE 
                    WHEN F_DATASIZEKB <= 1024 THEN '0-1MB'
                    WHEN F_DATASIZEKB <= 10240 THEN '1-10MB'
                    WHEN F_DATASIZEKB <= 102400 THEN '10-100MB'
                    WHEN F_DATASIZEKB <= 1024000 THEN '100MB-1GB'
                    ELSE '>1GB'
                END as size_range,
                COUNT(*) as count
            FROM TF_ORDER
            WHERE F_DATASIZEKB IS NOT NULL
            GROUP BY CASE 
                WHEN F_DATASIZEKB <= 1024 THEN '0-1MB'
                WHEN F_DATASIZEKB <= 10240 THEN '1-10MB'
                WHEN F_DATASIZEKB <= 102400 THEN '10-100MB'
                WHEN F_DATASIZEKB <= 1024000 THEN '100MB-1GB'
                ELSE '>1GB'
            END
            ORDER BY size_range
        """)
        size_stats = self.cursor.fetchall()
        
        return {
            "cloud_stats": cloud_stats,
            "size_stats": size_stats
        }
    
    def print_analysis_report(self):
        """打印分析报告"""
        # 获取所有统计信息
        basic_stats = self.get_basic_stats()
        user_prefs = self.get_user_preferences()
        quality_stats = self.get_data_quality_stats()
        
        # 打印基本统计
        print("=== 基本统计信息 ===")
        print(f"总订单数: {basic_stats['basic_stats']['total_orders']}")
        print(f"总用户数: {basic_stats['basic_stats']['total_users']}")
        print(f"卫星类型数: {basic_stats['basic_stats']['total_satellites']}")
        print("\n")
        
        # 打印用户活跃度TOP 10
        print("=== 最活跃用户TOP 10 ===")
        for user_id, order_count, first_order, last_order in basic_stats['user_stats'][:10]:
            print(f"用户ID: {user_id}, 订单数: {order_count}")
            print(f"首次订单: {first_order}, 最近订单: {last_order}")
        print("\n")
        
        # 打印卫星使用情况TOP 5
        print("=== 最受欢迎卫星TOP 5 ===")
        for satellite, usage_count, user_count in basic_stats['satellite_stats'][:5]:
            print(f"卫星: {satellite}")
            print(f"使用次数: {usage_count}, 使用用户数: {user_count}")
        print("\n")
        
        # 打印云量分布
        print("=== 云量分布 ===")
        for cloud_range, count in quality_stats['cloud_stats']:
            print(f"{cloud_range}: {count}订单")
        print("\n")
        
        # 打印数据大小分布
        print("=== 数据大小分布 ===")
        for size_range, count in quality_stats['size_stats']:
            print(f"{size_range}: {count}订单")