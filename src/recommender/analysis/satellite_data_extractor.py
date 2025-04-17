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
    
    def _execute_with_batched_ids(self, query_template, id_list, batch_size=900):
        """用批处理方式执行IN查询，避免ORA-01795错误
        
        Args:
            query_template: 查询模板，包含{}占位符
            id_list: ID列表
            batch_size: 每批的大小，默认900
            
        Returns:
            pd.DataFrame: 合并的查询结果
        """
        results = []
        
        # 将ID列表分批
        for i in range(0, len(id_list), batch_size):
            batch = id_list[i:i + batch_size]
            query = query_template.format(','.join([f"'{id}'" for id in batch]))
            
            self.cursor.execute(query)
            # 获取列名
            if not results:  # 只在第一批获取列名
                columns = [desc[0].lower() for desc in self.cursor.description]
            
            # 获取数据
            batch_data = self.cursor.fetchall()
            if batch_data:
                results.extend(batch_data)
        
        if not results:
            return pd.DataFrame()
            
        return pd.DataFrame(results, columns=columns)
        
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
        data = self.cursor.fetchall()
        if not data:
            return pd.DataFrame()
            
        order_data = pd.DataFrame(data, columns=columns)
        
        # 确保ID列是字符串类型
        order_data['f_id'] = order_data['f_id'].astype(str)
        
        return order_data
    
    def get_order_data_details(self, order_ids: List[str]) -> pd.DataFrame:
        """获取订单数据详细信息
        
        Args:
            order_ids: 订单ID列表
            
        Returns:
            pd.DataFrame: 包含订单数据详细信息的DataFrame
        """
        if not order_ids:
            return pd.DataFrame()
            
        # 使用批处理执行查询
        query_template = """
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
        """
        
        order_details = self._execute_with_batched_ids(query_template, order_ids)
        
        # 确保ID列是字符串类型
        if not order_details.empty:
            order_details['f_orderid'] = order_details['f_orderid'].astype(str)
            
        return order_details
    
    def get_satellite_metadata(self, data_ids: List[str], satellite_type: str) -> pd.DataFrame:
        """获取卫星元数据信息
        
        Args:
            data_ids: 数据ID列表
            satellite_type: 卫星类型
            
        Returns:
            pd.DataFrame: 包含卫星元数据的DataFrame
        """
        if not data_ids:
            return pd.DataFrame()
            
        # 根据卫星类型获取对应的元数据表
        metadata_table = self.satellite_table_map.get(satellite_type)
        if not metadata_table:
            print(f"警告: 未找到卫星类型 {satellite_type} 对应的元数据表")
            return pd.DataFrame()
            
        # 构建查询模板，使用批处理执行
        query_template = f"""
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
            WHERE F_DATAID IN ({{}})
        """
        
        metadata = self._execute_with_batched_ids(query_template, data_ids)
        
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
        if order_details.empty:
            print(f"未找到用户 {loginname} 的订单详细信息")
            return
        
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
        try:
            metadata = pd.concat(all_metadata, ignore_index=True)
        except Exception as e:
            print(f"合并元数据失败: {str(e)}")
            return
        
        # 4. 合并数据
        try:
            # 确保合并时的类型一致
            order_data['f_id'] = order_data['f_id'].astype(str)
            order_details['f_orderid'] = order_details['f_orderid'].astype(str)
            
            # 合并订单数据和订单详细信息
            merged_data = pd.merge(
                order_data,
                order_details,
                left_on='f_id',
                right_on='f_orderid',
                how='left'
            )
            
            # 合并元数据
            if 'f_dataid' in metadata.columns:
                final_data = pd.merge(
                    merged_data,
                    metadata,
                    on='f_dataid',  # 使用on而不是left_on/right_on确保类型一致
                    how='left'
                )
            else:
                final_data = merged_data
        except Exception as e:
            print(f"合并数据失败: {str(e)}")
            return
        
        # 5. 保存数据
        try:
            # 保存为CSV
            csv_path = f'{output_dir}/{loginname}_satellite_data.csv'
            final_data.to_csv(csv_path, index=False, encoding='utf-8-sig')  # 使用带BOM的UTF-8编码
            print(f"卫星数据已保存为CSV格式: {csv_path}")
            
            # 保存数据统计信息
            stats = {
                'total_orders': len(order_data),
                'total_data_items': len(order_details),
                'total_metadata_items': len(metadata) if not metadata.empty else 0,
                'unique_satellites': final_data['f_satelite'].nunique() if 'f_satelite' in final_data.columns else 0,
                'unique_sensors': final_data['f_sensor'].nunique() if 'f_sensor' in final_data.columns else 0
            }
            
            # 添加数据大小统计（如果列存在）
            if 'f_datasize' in final_data.columns and not final_data['f_datasize'].empty:
                stats['data_size_stats'] = {
                    'min': float(final_data['f_datasize'].min()),
                    'max': float(final_data['f_datasize'].max()),
                    'mean': float(final_data['f_datasize'].mean()),
                    'total': float(final_data['f_datasize'].sum())
                }
            
            # 添加云量统计（如果列存在）
            if 'f_cloudpercent' in final_data.columns and not final_data['f_cloudpercent'].empty:
                cloud_data = final_data['f_cloudpercent'].dropna()
                if not cloud_data.empty:
                    stats['cloud_percent_stats'] = {
                        'min': float(cloud_data.min()),
                        'max': float(cloud_data.max()),
                        'mean': float(cloud_data.mean())
                    }
            
            stats_path = f'{output_dir}/{loginname}_data_statistics.json'
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            print(f"数据统计信息已保存: {stats_path}")
            
            return final_data
        except Exception as e:
            print(f"保存数据时出错: {str(e)}")
            return None
    
    def batch_process_users(self, loginnames: List[str], output_dir: str = 'satellite_data'):
        """批量处理多个用户的卫星数据
        
        Args:
            loginnames: 用户登录名列表
            output_dir: 输出目录
        """
        # 创建输出目录
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"创建输出目录: {output_dir}")
            
        processed_count = 0
        error_count = 0
        
        for loginname in loginnames:
            print(f"\n处理用户 {loginname} 的卫星数据...")
            try:
                result = self.process_and_save_data(loginname, output_dir)
                if result is not None:
                    processed_count += 1
            except Exception as e:
                error_count += 1
                print(f"处理用户 {loginname} 的数据时出错: {str(e)}")
                
        print(f"\n批处理完成: 成功处理 {processed_count} 个用户，失败 {error_count} 个用户") 