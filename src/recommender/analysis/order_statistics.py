import oracledb
from typing import Dict, Any
import pandas as pd
from datetime import datetime

class OrderAnalyzer:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()

    # ... 保留原有方法 ...

    def get_user_detailed_stats(self, user_id: str) -> Dict[str, Any]:
        """获取单个用户的详细统计信息
        
        Args:
            user_id: 用户ID
            
        Returns:
            Dict包含用户的详细统计信息
        """
        # 1. 用户基本订单信息
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_orders,
                MIN(F_CREATTIME) as first_order_time,
                MAX(F_CREATTIME) as last_order_time,
                COUNT(DISTINCT F_SATELLITE) as used_satellites,
                COUNT(DISTINCT F_PROVINCESPACE) as covered_provinces,
                AVG(F_CLOUDAMOUNT) as avg_cloud_amount,
                AVG(F_DATASIZEKB) as avg_data_size,
                COUNT(DISTINCT F_DATATYPE) as data_types_used
            FROM TF_ORDER
            WHERE F_USERID = :user_id
        """, {'user_id': user_id})
        
        basic_stats = dict(zip([
            'total_orders', 'first_order_time', 'last_order_time', 
            'used_satellites', 'covered_provinces', 'avg_cloud_amount',
            'avg_data_size', 'data_types_used'
        ], self.cursor.fetchone()))
        
        # 2. 最喜欢的卫星及其使用频率
        self.cursor.execute("""
            SELECT 
                F_SATELLITE,
                COUNT(*) as usage_count,
                AVG(F_CLOUDAMOUNT) as avg_cloud,
                AVG(F_DATASIZEKB) as avg_size
            FROM TF_ORDER
            WHERE F_USERID = :user_id
                AND F_SATELLITE IS NOT NULL
            GROUP BY F_SATELLITE
            ORDER BY usage_count DESC
        """, {'user_id': user_id})
        
        favorite_satellites = [dict(zip([
            'satellite', 'usage_count', 'avg_cloud', 'avg_size'
        ], row)) for row in self.cursor.fetchall()]
        
        # 3. 地理偏好分析
        self.cursor.execute("""
            SELECT 
                F_PROVINCESPACE,
                COUNT(*) as region_count,
                AVG(F_CLOUDAMOUNT) as avg_cloud,
                COUNT(DISTINCT F_SATELLITE) as satellite_types
            FROM TF_ORDER
            WHERE F_USERID = :user_id
                AND F_PROVINCESPACE IS NOT NULL
            GROUP BY F_PROVINCESPACE
            ORDER BY region_count DESC
        """, {'user_id': user_id})
        
        region_preferences = [dict(zip([
            'province', 'order_count', 'avg_cloud', 'satellite_types'
        ], row)) for row in self.cursor.fetchall()]
        
        # 4. 用户行为分析
        self.cursor.execute("""
            SELECT 
                F_BEHAVIORTYPE,
                COUNT(*) as behavior_count,
                SUM(F_DATANUM) as total_data_num,
                SUM(F_DATASIZE) as total_data_size,
                MIN(F_CREATETIME) as first_time,
                MAX(F_CREATETIME) as last_time
            FROM JGF_GXFW.TB_USER_BEHAVIOR
            WHERE F_USERID = :user_id
            GROUP BY F_BEHAVIORTYPE
            ORDER BY behavior_count DESC
        """, {'user_id': user_id})
        
        behavior_stats = [dict(zip([
            'behavior_type', 'count', 'data_num', 'data_size', 
            'first_time', 'last_time'
        ], row)) for row in self.cursor.fetchall()]
        
        # 5. 每日行为统计
        self.cursor.execute("""
            SELECT 
                F_CREATETIME,
                F_DATASIZE,
                F_DATANUM,
                F_SEARCHNUM
            FROM JGF_GXFW.TB_USER_BEHAVIOR_BYDAY
            WHERE F_USERID = :user_id
            ORDER BY F_CREATETIME DESC
        """, {'user_id': user_id})
        
        daily_stats = [dict(zip([
            'date', 'data_size', 'data_num', 'search_num'
        ], row)) for row in self.cursor.fetchall()]
        
        return {
            "basic_stats": basic_stats,
            "favorite_satellites": favorite_satellites,
            "region_preferences": region_preferences,
            "behavior_stats": behavior_stats,
            "daily_stats": daily_stats
        }
    
    def print_user_detailed_report(self, user_id: str):
        """打印用户详细报告"""
        stats = self.get_user_detailed_stats(user_id)
        
        print(f"\n=== 用户 {user_id} 详细报告 ===\n")
        
        # 基本统计
        basic = stats['basic_stats']
        print("基本信息:")
        print(f"总订单数: {basic['total_orders']}")
        print(f"首次订单: {basic['first_order_time']}")
        print(f"最近订单: {basic['last_order_time']}")
        print(f"使用卫星数: {basic['used_satellites']}")
        print(f"覆盖省份数: {basic['covered_provinces']}")
        print(f"平均云量: {basic['avg_cloud_amount']:.2f}%")
        print(f"平均数据大小: {basic['avg_data_size']/1024:.2f} MB")
        print(f"使用数据类型数: {basic['data_types_used']}")
        
        # 最喜欢的卫星
        print("\n最喜欢的卫星 (TOP 5):")
        for sat in stats['favorite_satellites'][:5]:
            print(f"卫星: {sat['satellite']}")
            print(f"  使用次数: {sat['usage_count']}")
            print(f"  平均云量: {sat['avg_cloud']:.2f}%")
            print(f"  平均数据大小: {sat['avg_size']/1024:.2f} MB")
        
        # 地区偏好
        print("\n地区偏好 (TOP 5):")
        for region in stats['region_preferences'][:5]:
            print(f"省份: {region['province']}")
            print(f"  订单数: {region['order_count']}")
            print(f"  使用卫星类型数: {region['satellite_types']}")
            print(f"  平均云量: {region['avg_cloud']:.2f}%")
        
        # 行为统计
        print("\n用户行为统计:")
        behavior_types = {
            0: "注册", 1: "登录", 2: "查询", 
            3: "加入购物车", 4: "创建订单", 
            5: "下载数据", 6: "用户访问"
        }
        for behavior in stats['behavior_stats']:
            behavior_name = behavior_types.get(behavior['behavior_type'], str(behavior['behavior_type']))
            print(f"{behavior_name}:")
            print(f"  次数: {behavior['count']}")
            if behavior['data_num']:
                print(f"  数据量: {behavior['data_num']} 条")
            if behavior['data_size']:
                print(f"  数据大小: {behavior['data_size']/1024:.2f} MB")
        
        # 最近活动
        print("\n最近活动统计 (最近5天):")
        for daily in stats['daily_stats'][:5]:
            print(f"日期: {daily['date']}")
            print(f"  搜索次数: {daily['search_num']}")
            print(f"  数据量: {daily['data_num']} 条")
            print(f"  数据大小: {daily['data_size']/1024:.2f} MB")

    def print_analysis_report(self):
        """打印分析报告"""
        # ... 保留原有代码 ...
        
        # 添加示例用户的详细分析
        print("\n=== 示例用户详细分析 ===")
        # 获取最活跃的用户
        self.cursor.execute("""
            SELECT F_USERID, COUNT(*) as order_count
            FROM TF_ORDER
            GROUP BY F_USERID
            ORDER BY order_count DESC
            FETCH FIRST 1 ROW ONLY
        """)
        top_user = self.cursor.fetchone()
        if top_user:
            self.print_user_detailed_report(top_user[0])