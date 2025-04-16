import pandas as pd
import numpy as np
from typing import Dict, List, Any
import os
import json
from datetime import datetime

class SatelliteDataExtractor:
    """卫星数据提取器，用于从订单数据中提取卫星数据信息"""
    
    def __init__(self, conn):
        """初始化提取器
        
        Args:
            conn: Oracle数据库连接
        """
        self.conn = conn
        self.cursor = conn.cursor()
        
        # 定义卫星类型与元数据表的映射关系
        self.satellite_table_map = {
            # GF1系列
            "GF1_WFV_YSDATA": "TB_META_GF1",
            "GF1_YSDATA": "TB_META_GF1",
            "GF1B_YSDATA": "TB_META_GF1B",
            "GF1C_YSDATA": "TB_META_GF1C",
            "GF1D_YSDATA": "TB_META_GF1D",
            # GF2系列
            "GF2_YSDATA": "TB_META_GF2",
            # GF5系列
            "GF5_AHSIDATA": "TB_META_GF5",
            "GF5_VIMSDATA": "TB_META_GF5",
            # GF6系列
            "GF6_WFV_DATA": "TB_META_GF6",
            "GF6_YSDATA": "TB_META_GF6",
            # GF7系列
            "GF7_BWD_DATA": "TB_META_GF7",
            "GF7_MUX_DATA": "TB_META_GF7",
            # ZY301系列
            "ZY301A_MUX_DATA": "TB_META_ZY301",
            "ZY301A_NAD_DATA": "TB_META_ZY301",
            # ZY302系列
            "ZY302A_MUX_DATA": "TB_META_ZY302",
            "ZY302A_NAD_DATA": "TB_META_ZY302",
            # ZY303系列
            "ZY303A_MUX_DATA": "TB_META_ZY303",
            "ZY303A_NAD_DATA": "TB_META_ZY303",
            # ZY02C系列
            "ZY02C_HRC_DATA": "TB_META_ZY02C",
            "ZY02C_PMS_DATA": "TB_META_ZY02C",
            # ZY1E系列
            "ZY1E_AHSI": "TB_META_ZY1E",
            # ZY1F系列
            "ZY1F_AHSI": "TB_META_ZY1F",
            "ZY1F_ISR_NSR": "TB_META_ZY1F",
            # CB04A系列
            "CB04A_VNIC": "TB_META_CB04A",
        }
        
    def get_order_data(self, loginname: str) -> pd.DataFrame:
        """获取指定用户的订单数据
        
        Args:
            loginname: 用户登录名
            
        Returns:
            pd.DataFrame: 包含订单数据的DataFrame
        """
        # 查询TF_ORDER表获取订单信息
        self.cursor.execute("""
            SELECT 
                F_ID,
                F_ORDERNAME,
                F_CREATTIME,
                F_SATELLITE,
                F_SENSOR,
                F_CLOUDAMOUNT,
                F_DATASIZEKB,
                F_PROVINCESPACE,
                F_DATATYPE
            FROM TF_ORDER
            WHERE F_LOGIN_USER = :loginname
        """, {'loginname': loginname})
        
        # 获取列名
        columns = [desc[0].lower() for desc in self.cursor.description]
        # 获取数据
        order_data = pd.DataFrame(self.cursor.fetchall(), columns=columns)
        
        return order_data
    
    def get_order_data_details(self, order_ids: List[str]) -> pd.DataFrame:
        """获取订单数据详细信息
        
        Args:
            order_ids: 订单ID列表
            
        Returns:
            pd.DataFrame: 包含订单数据详细信息的DataFrame
        """
        # 查询TF_ORDERDATA表获取数据详细信息
        self.cursor.execute("""
            SELECT 
                F_ORDERID,
                F_DATAID,
                F_DATANAME,
                F_SATELITE,
                F_SENSOR,
                F_RECEIVETIME,
                F_DATASIZE,
                F_CLOUDPERCENT,
                F_ORBITID,
                F_SCENEID
            FROM TF_ORDERDATA
            WHERE F_ORDERID IN ({})
        """.format(','.join([f"'{id}'" for id in order_ids])))
        
        # 获取列名
        columns = [desc[0].lower() for desc in self.cursor.description]
        # 获取数据
        order_details = pd.DataFrame(self.cursor.fetchall(), columns=columns)
        
        return order_details
    
    def get_satellite_metadata(self, data_ids: List[str], satellite_type: str) -> pd.DataFrame:
        """获取卫星元数据信息
        
        Args:
            data_ids: 数据ID列表
            satellite_type: 卫星类型
            
        Returns:
            pd.DataFrame: 包含卫星元数据的DataFrame
        """
        # 根据卫星类型获取对应的元数据表
        metadata_table = self.satellite_table_map.get(satellite_type)
        if not metadata_table:
            print(f"警告: 未找到卫星类型 {satellite_type} 对应的元数据表")
            return pd.DataFrame()
            
        # 查询对应的元数据表
        self.cursor.execute(f"""
            SELECT 
                F_DATANAME,
                F_PRODUCETIME,
                F_PRODUCTID,
                F_DATAID,
                F_PRODUCTLEVEL,
                F_SATELLITEID,
                F_SENSORID,
                F_CLOUDPERCENT,
                F_ORBITID,
                F_SCENEID,
                F_SCENEPATH,
                F_SCENEROW,
                F_RECEIVETIME,
                F_DATASIZE,
                F_DATATYPENAME,
                F_LOCATION,
                F_PITCHSATELLITEANGLE,
                F_PITCHVIEWINGANGLE,
                F_YAWSATELLITEANGLE,
                F_ROLLSATELLITEANGLE,
                F_ROLLVIEWINGANGLE
            FROM {metadata_table}
            WHERE F_DATAID IN ({','.join([f"'{id}'" for id in data_ids])})
        """)
        
        # 获取列名
        columns = [desc[0].lower() for desc in self.cursor.description]
        # 获取数据
        metadata = pd.DataFrame(self.cursor.fetchall(), columns=columns)
        
        return metadata
    
    def process_and_save_data(self, loginname: str, output_dir: str = 'satellite_data'):
        """处理并保存卫星数据信息
        
        Args:
            loginname: 用户登录名
            output_dir: 输出目录
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 1. 获取订单数据
        order_data = self.get_order_data(loginname)
        if order_data.empty:
            print(f"未找到用户 {loginname} 的订单数据")
            return
        
        # 2. 获取订单详细信息
        order_details = self.get_order_data_details(order_data['f_id'].tolist())
        
        # 3. 按卫星类型分组获取元数据
        all_metadata = []
        for satellite_type, group in order_details.groupby('f_satelite'):
            data_ids = group['f_dataid'].tolist()
            metadata = self.get_satellite_metadata(data_ids, satellite_type)
            if not metadata.empty:
                metadata['f_satelite'] = satellite_type  # 添加卫星类型列
                all_metadata.append(metadata)
        
        if not all_metadata:
            print("未找到任何卫星元数据")
            return
            
        # 合并所有元数据
        metadata = pd.concat(all_metadata, ignore_index=True)
        
        # 4. 合并数据
        # 合并订单数据和订单详细信息
        merged_data = pd.merge(
            order_data,
            order_details,
            left_on='f_id',
            right_on='f_orderid',
            how='left'
        )
        
        # 合并元数据
        final_data = pd.merge(
            merged_data,
            metadata,
            left_on='f_dataid',
            right_on='f_dataid',
            how='left'
        )
        
        # 5. 保存数据
        # 保存为CSV
        csv_path = f'{output_dir}/{loginname}_satellite_data.csv'
        final_data.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"卫星数据已保存为CSV格式: {csv_path}")
        
        # 保存数据统计信息
        stats = {
            'total_orders': len(order_data),
            'total_data_items': len(order_details),
            'total_metadata_items': len(metadata),
            'unique_satellites': final_data['f_satelite'].nunique(),
            'unique_sensors': final_data['f_sensor'].nunique(),
            'data_size_stats': {
                'min': final_data['f_datasize'].min(),
                'max': final_data['f_datasize'].max(),
                'mean': final_data['f_datasize'].mean(),
                'total': final_data['f_datasize'].sum()
            },
            'cloud_percent_stats': {
                'min': final_data['f_cloudpercent'].min(),
                'max': final_data['f_cloudpercent'].max(),
                'mean': final_data['f_cloudpercent'].mean()
            }
        }
        
        stats_path = f'{output_dir}/{loginname}_data_statistics.json'
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        print(f"数据统计信息已保存: {stats_path}")
        
        return final_data
    
    def batch_process_users(self, loginnames: List[str], output_dir: str = 'satellite_data'):
        """批量处理多个用户的卫星数据
        
        Args:
            loginnames: 用户登录名列表
            output_dir: 输出目录
        """
        for loginname in loginnames:
            print(f"\n处理用户 {loginname} 的卫星数据...")
            try:
                self.process_and_save_data(loginname, output_dir)
            except Exception as e:
                print(f"处理用户 {loginname} 的数据时出错: {str(e)}") 