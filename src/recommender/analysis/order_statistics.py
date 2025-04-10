import oracledb
from typing import Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
import sys

class OrderAnalyzer:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()


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
            WHERE F_LOGIN_USER = :user_id
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
            WHERE F_LOGIN_USER = :user_id
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
            WHERE F_LOGIN_USER = :user_id
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
        print(f"平均云量: {basic['avg_cloud_amount']:.2f}%" if basic['avg_cloud_amount'] is not None else "平均云量: 暂无数据")
        print(f"平均数据大小: {basic['avg_data_size']/1024:.2f} MB" if basic['avg_data_size'] is not None else "平均数据大小: 暂无数据")
        print(f"使用数据类型数: {basic['data_types_used']}")
        
        # 最喜欢的卫星
        print("\n最喜欢的卫星 (TOP 5):")
        for sat in stats['favorite_satellites'][:5]:
            print(f"卫星: {sat['satellite']}")
            print(f"  使用次数: {sat['usage_count']}")
            print(f"  平均云量: {sat['avg_cloud']:.2f}%" if sat['avg_cloud'] is not None else "  平均云量: 暂无数据")
            print(f"  平均数据大小: {sat['avg_size']/1024:.2f} MB" if sat['avg_size'] is not None else "  平均数据大小: 暂无数据")
        
        # 地区偏好
        print("\n地区偏好 (TOP 5):")
        for region in stats['region_preferences'][:5]:
            print(f"省份: {region['province']}")
            print(f"  订单数: {region['order_count']}")
            print(f"  使用卫星类型数: {region['satellite_types']}")
            print(f"  平均云量: {region['avg_cloud']:.2f}%" if region['avg_cloud'] is not None else "  平均云量: 暂无数据")
        
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
            print(f"  搜索次数: {daily['search_num'] or 0}")
            print(f"  数据量: {daily['data_num'] or 0} 条")
            print(f"  数据大小: {(daily['data_size'] or 0)/1024:.2f} MB")

    def get_user_features(self) -> pd.DataFrame:
        """获取所有用户的特征，用于后续的深度学习模型训练
        
        Returns:
            pd.DataFrame: 包含所有用户特征的DataFrame
        """
        # 1. 获取用户基本统计特征
        self.cursor.execute("""
            WITH user_basic_stats AS (
                SELECT 
                    F_LOGIN_USER,
                    COUNT(*) as total_orders,
                    MIN(F_CREATTIME) as first_order_time,
                    MAX(F_CREATTIME) as last_order_time,
                    COUNT(DISTINCT F_SATELLITE) as used_satellites,
                    COUNT(DISTINCT F_PROVINCESPACE) as covered_provinces,
                    AVG(F_CLOUDAMOUNT) as avg_cloud_amount,
                    AVG(F_DATASIZEKB) as avg_data_size,
                    COUNT(DISTINCT F_DATATYPE) as data_types_used,
                    -- 添加更多时间相关特征
                    ROUND((CAST(MAX(F_CREATTIME) AS DATE) - CAST(MIN(F_CREATTIME) AS DATE))) as active_days,
                    -- 添加数据大小相关特征
                    MIN(F_DATASIZEKB) as min_data_size,
                    MAX(F_DATASIZEKB) as max_data_size,
                    STDDEV(F_DATASIZEKB) as std_data_size,
                    -- 添加云量相关特征
                    MIN(F_CLOUDAMOUNT) as min_cloud_amount,
                    MAX(F_CLOUDAMOUNT) as max_cloud_amount,
                    STDDEV(F_CLOUDAMOUNT) as std_cloud_amount
                FROM TF_ORDER
                GROUP BY F_LOGIN_USER
            )
            SELECT * FROM user_basic_stats
        """)
        
        # 获取列名
        columns = [desc[0].lower() for desc in self.cursor.description]
        # 获取数据
        basic_features = pd.DataFrame(self.cursor.fetchall(), columns=columns)
        
        # 2. 获取用户的卫星使用分布
        self.cursor.execute("""
            SELECT 
                F_LOGIN_USER,
                F_SATELLITE,
                COUNT(*) as usage_count
            FROM TF_ORDER
            WHERE F_SATELLITE IS NOT NULL
            GROUP BY F_LOGIN_USER, F_SATELLITE
        """)
        
        satellite_usage = pd.DataFrame(self.cursor.fetchall(), 
                                     columns=['f_login_user', 'satellite', 'usage_count'])
        # 将卫星使用转换为one-hot编码
        satellite_features = pd.pivot_table(satellite_usage, 
                                          values='usage_count',
                                          index='f_login_user',
                                          columns='satellite',
                                          fill_value=0)
        satellite_features.columns = [f'satellite_{col}' for col in satellite_features.columns]
        
        # 3. 获取用户的地区使用分布
        self.cursor.execute("""
            SELECT 
                F_LOGIN_USER,
                F_PROVINCESPACE,
                COUNT(*) as usage_count
            FROM TF_ORDER
            WHERE F_PROVINCESPACE IS NOT NULL
            GROUP BY F_LOGIN_USER, F_PROVINCESPACE
        """)
        
        province_usage = pd.DataFrame(self.cursor.fetchall(), 
                                    columns=['f_login_user', 'province', 'usage_count'])
        # 将地区使用转换为one-hot编码
        province_features = pd.pivot_table(province_usage, 
                                         values='usage_count',
                                         index='f_login_user',
                                         columns='province',
                                         fill_value=0)
        province_features.columns = [f'province_{col}' for col in province_features.columns]
        
        # 合并所有特征
        features = basic_features.set_index('f_login_user')
        if not satellite_features.empty:
            features = features.join(satellite_features, how='left')
        if not province_features.empty:
            features = features.join(province_features, how='left')
        
        # 填充缺失值
        features = features.fillna(0)
        
        return features
    
    def save_features_for_embedding(self, output_dir: str = 'user_features'):
        """保存用户特征用于后续的embedding训练
        
        Args:
            output_dir: 输出目录
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 获取用户特征
        features = self.get_user_features()
        
        # 1. 保存为CSV格式（基本格式，确保可用）
        csv_path = f'{output_dir}/user_features.csv'
        features.to_csv(csv_path, encoding='utf-8')  # 使用带BOM的UTF-8编码
        print(f"特征数据已保存为CSV格式: {csv_path}")

        # 2. 尝试保存为Parquet格式（如果有相关依赖）
        try:
            parquet_path = f'{output_dir}/user_features.parquet'
            features.to_parquet(parquet_path)
            print(f"特征数据已保存为Parquet格式: {parquet_path}")
        except ImportError:
            print("注意: 未安装parquet相关依赖(pyarrow或fastparquet)，跳过parquet格式保存")
            print("如需parquet格式支持，请安装依赖: pip install pyarrow")      
       
        # 3. 保存特征信息（列名和数据类型）
        feature_info = {
            'columns': list(features.columns),
            'dtypes': {col: str(dtype) for col, dtype in features.dtypes.items()}
        }
        info_path = f'{output_dir}/feature_info.json'
        with open(info_path, 'w', encoding='utf-8') as f:
            json.dump(feature_info, f, indent=2, ensure_ascii=False)
        print(f"特征信息已保存: {info_path}")
        
        # 4. 生成特征统计信息
        stats_path = f'{output_dir}/feature_statistics.csv'
        stats = features.describe()
        stats.to_csv(stats_path)
        print(f"特征统计信息已保存: {stats_path}")
        
        print(f"\n=== 特征导出信息 ===")
        print(f"特征维度: {features.shape}")
        print(f"数值型特征数: {features.select_dtypes(include=[np.number]).shape[1]}")
        print(f"类别型特征数: {features.select_dtypes(exclude=[np.number]).shape[1]}")
        
        # 5. 生成示例代码
        example_code = f'''
# 示例：如何加载和使用这些特征

import pandas as pd
import tensorflow as tf

# 1. 加载特征数据
features = pd.read_csv('user_features.csv', index_col=0)  # 使用CSV格式加载

# 2. 转换为TensorFlow数据集
def df_to_dataset(dataframe, shuffle=True, batch_size=32):
    df = dataframe.copy()
    ds = tf.data.Dataset.from_tensor_slices(dict(df))
    if shuffle:
        ds = ds.shuffle(buffer_size=len(df))
    ds = ds.batch(batch_size)
    return ds

# 3. 创建特征列
feature_columns = []

# 数值型特征
numeric_features = {list(features.select_dtypes(include=[np.number]).columns)}
for feature_name in numeric_features:
    feature_columns.append(tf.feature_column.numeric_column(feature_name))

# 4. 创建模型
model = tf.keras.Sequential([
    tf.keras.layers.DenseFeatures(feature_columns),
    tf.keras.layers.Dense(128, activation='relu'),
    tf.keras.layers.Dense(64, activation='relu'),
    tf.keras.layers.Dense(32)  # 最终的embedding维度
])
'''
        
        example_path = f'{output_dir}/example_usage.py'
        with open(example_path, 'w', encoding='utf-8') as f:
            f.write(example_code)
        print(f"示例代码已保存: {example_path}")
        
        print("\n特征数据已成功导出！您可以在示例代码中查看如何使用这些特征。")


    def print_analysis_report(self):
        """打印分析报告"""
        # ... 保留原有代码 ...
        # 获取所有用户的详细统计信息
        print("=== 所有用户详细统计信息 ===")
        self.cursor.execute("""
            WITH user_basic_stats AS (
                SELECT 
                    F_LOGIN_USER,
                    COUNT(*) as total_orders,
                    MIN(F_CREATTIME) as first_order_time,
                    MAX(F_CREATTIME) as last_order_time,
                    COUNT(DISTINCT F_SATELLITE) as used_satellites,
                    COUNT(DISTINCT F_PROVINCESPACE) as covered_provinces,
                    AVG(F_CLOUDAMOUNT) as avg_cloud_amount,
                    AVG(F_DATASIZEKB) as avg_data_size,
                    COUNT(DISTINCT F_DATATYPE) as data_types_used
                FROM TF_ORDER
                GROUP BY F_LOGIN_USER
            )
            SELECT 
                u.F_LOGIN_USER,
                u.total_orders,
                u.first_order_time,
                u.last_order_time,
                u.used_satellites,
                u.covered_provinces,
                u.avg_cloud_amount,
                u.avg_data_size,
                u.data_types_used
            FROM user_basic_stats u
            ORDER BY u.total_orders DESC
        """)
        
        users_stats = self.cursor.fetchall()
        for user_stats in users_stats:
            user_id = user_stats[0]
            total_orders = user_stats[1]
            first_order = user_stats[2]
            last_order = user_stats[3]
            used_satellites = user_stats[4]
            covered_provinces = user_stats[5]
            avg_cloud = user_stats[6]
            avg_size = user_stats[7]
            data_types = user_stats[8]
            
            print(f"\n用户 {user_id} 的统计信息:")
            print(f"  总订单数: {total_orders}")
            print(f"  首次订单: {first_order}")
            print(f"  最近订单: {last_order}")
            print(f"  使用卫星数: {used_satellites}")
            print(f"  覆盖省份数: {covered_provinces}")
            print(f"  平均云量: {avg_cloud:.2f}%" if avg_cloud is not None else "  平均云量: 暂无数据")
            print(f"  平均数据大小: {avg_size/1024:.2f} MB" if avg_size is not None else "  平均数据大小: 暂无数据")
            print(f"  使用数据类型数: {data_types}")

            # 获取用户的卫星使用情况
            self.cursor.execute("""
                SELECT 
                    F_SATELLITE,
                    COUNT(*) as usage_count,
                    AVG(F_CLOUDAMOUNT) as avg_cloud,
                    COUNT(*) OVER () as total_satellites
                FROM TF_ORDER
                WHERE F_LOGIN_USER = :user_id
                    AND F_SATELLITE IS NOT NULL
                GROUP BY F_SATELLITE
                ORDER BY COUNT(*) DESC
                FETCH FIRST 5 ROWS ONLY
            """, {'user_id': user_id})
            
            satellite_stats = self.cursor.fetchall()

            if satellite_stats:
                total_satellites = satellite_stats[0][3]  # 获取总卫星数
                print(f"  卫星使用情况 (共{total_satellites}颗):")
                for i, (satellite, count, avg_cloud, _) in enumerate(satellite_stats, 1):
                    cloud_str = f", 平均云量: {avg_cloud:.2f}%" if avg_cloud is not None else ", 平均云量: 暂无数据"
                    print(f"    {i}. {satellite}({count}次{cloud_str})")
                if total_satellites > 5:
                    print(f"    ... 还有 {total_satellites - 5} 颗卫星未显示")

            # 获取用户的地区使用情况
            self.cursor.execute("""
                SELECT 
                    F_PROVINCESPACE,
                    COUNT(*) as usage_count,
                    COUNT(*) OVER () as total_provinces
                FROM TF_ORDER
                WHERE F_LOGIN_USER = :user_id
                    AND F_PROVINCESPACE IS NOT NULL
                GROUP BY F_PROVINCESPACE
                ORDER BY COUNT(*) DESC
                FETCH FIRST 5 ROWS ONLY
            """, {'user_id': user_id})
            
            province_stats = self.cursor.fetchall()
            
            if province_stats:
                total_provinces = province_stats[0][2]  # 获取总地区数
                print(f"  地区使用情况 (共{total_provinces}个地区):")
                for i, (province, count, _) in enumerate(province_stats, 1):
                    print(f"    {i}. {province}({count}次)")
                if total_provinces > 5:
                    print(f"    ... 还有 {total_provinces - 5} 个地区未显示")
            # 在最后添加特征导出
        print("\n=== 导出用户特征数据 ===")
        self.save_features_for_embedding()


        # # 添加示例用户的详细分析
        # print("\n=== 示例用户详细分析 ===")
        # # 获取最活跃的用户
        # self.cursor.execute("""
        #     SELECT F_LOGIN_USER, COUNT(*) as order_count
        #     FROM TF_ORDER
        #     GROUP BY F_LOGIN_USER
        #     ORDER BY COUNT(*) DESC
        #     FETCH FIRST 1 ROW ONLY
        # """)
        # top_user = self.cursor.fetchone()
        # if top_user:
        #     self.print_user_detailed_report(top_user[0])



