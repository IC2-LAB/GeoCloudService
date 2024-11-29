from datetime import datetime
import os
import json
import src.utils.db.mapper as mapper
from src.utils.logger import logger
from oracledb import DbObject
from src.utils.db.mapper import serialize 

# def serialize(obj):
#     """
#     将对象序列化为可 JSON 化的格式
#     """
#     if isinstance(obj, datetime):
#         return obj.isoformat()
#     elif isinstance(obj, DbObject):
#         # 尝试将 DbObject 转换为字典
#         # 获取所有公共属性，过滤掉方法
#         return {attr: serialize(getattr(obj, attr)) for attr in dir(obj) 
#                 if not attr.startswith('_') and not callable(getattr(obj, attr))}
#     # 处理其他应该被序列化的类型
#     elif isinstance(obj, list):
#         return [serialize(item) for item in obj]  # 处理列表
#     elif isinstance(obj, dict):
#         return {key: serialize(value) for key, value in obj.items()}  # 处理字典
#     raise TypeError(f"Type {type(obj)} not serializable")



# class SatelliteDataProcess:
#     def __init__(self, pool):
#         self.mapper = mapper.Mapper(pool)

#     def process_directory(self, directory, target_folder):
#         # 使用反斜杠拼接路径
#         target_path = directory + '\\' + target_folder
#         logger.info(f"检查目标路径: {target_path}")
        
#         if os.path.exists(target_path) and os.path.isdir(target_path):
#             for filename in os.listdir(target_path):
#                 if filename.endswith('.json'):
#                     json_file_path = target_path + '\\' + filename
#                     self.store_json_to_db(json_file_path, target_folder)  # 将文件路径和文件夹名称传入
#         else:
#             logger.error(f"未找到目标文件夹: {target_path}")

#     def store_json_to_db(self, json_file_path, folder_name):
#         try:
#             # 读取 JSON 文件内容
#             with open(json_file_path, 'r', encoding='utf-8') as json_file:
#                 record = json.load(json_file)

#                 # 打印读取的记录以便调试
#                 logger.info(f"读取数据: {record}")

#                 # 映射关系，文件夹名-->数据库表名
#                 table_mapping = {
#                     # GF1:
#                     "GF1_WFV_YSDATA": "TB_META_GF1",
#                     "GF1_YSDATA": "TB_META_GF1",
#                     # GF1B:
#                     "GF1B_YSDATA": "TB_META_GF1B",
#                     # GF1C:
#                     "GF1C_YSDATA": "TB_META_GF1C",
#                     #GF1D:
#                     "GF1D_YSDATA": "TB_META_GF1D",
#                     # GF2:
#                     "GF2_YSDATA": "TB_META_GF2",
#                     # GF5:
#                     "GF5_AHSIDATA": "TB_META_GF5",
#                     "GF5_VIMSDATA": "TB_META_GF5",
#                     # GF6:
#                     "GF6_WFV_DATA": "TB_META_GF6",
#                     "GF6_YSDATA": "TB_META_GF6",
#                     # GF7:
#                     "GF7_BWD_DATA": "TB_META_GF7",
#                     "GF7_MUX_DATA": "TB_META_GF7",
#                     #ZY301:
#                     "ZY301A_MUX_DATA": "TB_META_ZY301",
#                     "ZY301A_NAD_DATA": "TB_META_ZY301",
#                     #ZY302:
#                     "ZY302A_MUX_DATA": "TB_META_ZY302",
#                     "ZY302A_NAD_DATA": "TB_META_ZY302",
#                     #ZY303: 
#                     "ZY303A_MUX_DATA": "TB_META_ZY303",
#                     "ZY303A_NAD_DATA": "TB_META_ZY303",
#                     #ZY02C:
#                     "ZY02C_HRC_DATA": "TB_META_ZY02C",
#                     "ZY02C_PMS_DATA": "TB_META_ZY02C",
#                     #ZY02D:

#                     #ZY1E:
#                     "ZY1E_AHSI": "TB_META_ZY1E",
#                     #ZY1F+:
#                     "ZY1F_AHSI": "TB_META_ZY1F",
#                     "ZY1F_ISR_NSR": "TB_META_ZY1F",
#                     #(ZY02E):

#                     #CB04A:
#                     "CB04A_VNIC": "TB_META_CB04A",
#                     #CB04B:


#                 }

#                 # 查找对应的表名
#                 table_name = table_mapping.get(folder_name)
#                 if table_name:
#                     # 假设存在一个方法插入数据到指定表
#                     self.mapper.insert_data_into_table(table_name, record)
#                     logger.info(f"{json_file_path} 数据存储成功到表 {table_name}")
#                 else:
#                     logger.error(f"未找到映射表名，文件夹: {folder_name}")

#         except Exception as e:
#             logger.error(f"{json_file_path} 数据存储失败: {e}")



#cmm1126修改
import os
import json
from src.utils.logger import logger

class SatelliteDataProcess:
    def __init__(self, pool):
        self.mapper = mapper.Mapper(pool)
        # 定义文件夹名与外网数据库表的映射关系
        self.folder_table_mapping = {
                    # GF1:
                    "GF1_WFV_YSDATA": "TB_META_GF1",
                    "GF1_YSDATA": "TB_META_GF1",
                    # GF1B:
                    "GF1B_YSDATA": "TB_META_GF1B",
                    # GF1C:
                    "GF1C_YSDATA": "TB_META_GF1C",
                    #GF1D:
                    "GF1D_YSDATA": "TB_META_GF1D",
                    # GF2:
                    "GF2_YSDATA": "TB_META_GF2",
                    # GF5:
                    "GF5_AHSIDATA": "TB_META_GF5",
                    "GF5_VIMSDATA": "TB_META_GF5",
                    # GF6:
                    "GF6_WFV_DATA": "TB_META_GF6",
                    "GF6_YSDATA": "TB_META_GF6",
                    # GF7:
                    "GF7_BWD_DATA": "TB_META_GF7",
                    "GF7_MUX_DATA": "TB_META_GF7",
                    #ZY301:
                    "ZY301A_MUX_DATA": "TB_META_ZY301",
                    "ZY301A_NAD_DATA": "TB_META_ZY301",
                    #ZY302:
                    "ZY302A_MUX_DATA": "TB_META_ZY302",
                    "ZY302A_NAD_DATA": "TB_META_ZY302",
                    #ZY303: 
                    "ZY303A_MUX_DATA": "TB_META_ZY303",
                    "ZY303A_NAD_DATA": "TB_META_ZY303",
                    #ZY02C:
                    "ZY02C_HRC_DATA": "TB_META_ZY02C",
                    "ZY02C_PMS_DATA": "TB_META_ZY02C",
                    #ZY02D:

                    #ZY1E:
                    "ZY1E_AHSI": "TB_META_ZY1E",
                    #ZY1F+:
                    "ZY1F_AHSI": "TB_META_ZY1F",
                    "ZY1F_ISR_NSR": "TB_META_ZY1F",
                    #(ZY02E):

                    #CB04A:
                    "CB04A_VNIC": "TB_META_CB04A",
                    #CB04B:


                }
        
    def process_directory(self, directory_path, folder_name):
        """
        处理指定目录下的数据并同步到对应的外网数据库表
        :param directory_path: 源目录路径
        :param folder_name: 文件夹名称
        """
        try:
            # 获取对应的外网表名
            external_table = self.folder_table_mapping.get(folder_name)
            if not external_table:
                logger.error(f"找不到文件夹 {folder_name} 对应的外网表映射")
                return

            folder_path = os.path.join(directory_path, folder_name)
            if not os.path.exists(folder_path):
                logger.warning(f"目录不存在: {folder_path}")
                return

            # 处理目录中的所有JSON文件
            success_count = 0
            total_count = 0
            for file_name in os.listdir(folder_path):
                if not file_name.endswith('.json'):
                    continue

                total_count += 1
                file_path = os.path.join(folder_path, file_name)
                
                try:
                    # 读取JSON文件
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # 插入数据到外网数据库对应的表
                    self.mapper.insert_data_into_table(external_table, data)
                    success_count += 1
                    logger.info(f"成功处理文件: {file_name}")
                    
                except Exception as e:
                    logger.error(f"处理文件 {file_name} 失败: {str(e)}")

            logger.info(f"文件夹 {folder_name} 处理完成: 总计 {total_count} 个文件，成功 {success_count} 个")

        except Exception as e:
            logger.error(f"处理目录 {folder_name} 时发生错误: {str(e)}")

    # def sync_data_to_disk(self, start_time, end_time, table_names, target_path):
    #     """同步数据到网盘"""
    #     try:
    #         total_sync_count = 0
    #         successful_tables = []
    #         failed_tables = []
            
    #         for table_name in table_names:
    #             try:
    #                 # 1. 查询数据库获取数据
    #                 data_list = self.mapper.getDataByReceiveTime(start_time, end_time, table_name)
    #                 logger.info(f"从表 {table_name} 获取到 {len(data_list)} 条数据")
                    
    #                 # 2. 创建目标目录
    #                 table_path = os.path.join(target_path, table_name)
    #                 os.makedirs(table_path, exist_ok=True)
                    
    #                 # 3. 获取同步前的文件数量
    #                 before_count = len(os.listdir(table_path)) if os.path.exists(table_path) else 0
                    
    #                 # 4. 保存数据为JSON文件
    #                 success_count = 0
    #                 for data in data_list:
    #                     try:
    #                         file_name = f"{table_name}-{data['F_DATANAME']}.json"
    #                         file_path = os.path.join(table_path, file_name)
                            
    #                         if not os.path.exists(file_path):
    #                             with open(file_path, 'w', encoding='utf-8') as f:
    #                                 json.dump(data, f, ensure_ascii=False, indent=4, default=str)
    #                             success_count += 1
                                
    #                     except Exception as e:
    #                         logger.error(f"保存文件 {file_name} 失败: {str(e)}")
                    
    #                 # 5. 获取同步后的文件数量并计算新增数量
    #                 after_count = len(os.listdir(table_path)) if os.path.exists(table_path) else 0
    #                 synced_count = after_count - before_count
                    
    #                 total_sync_count += synced_count
    #                 successful_tables.append((table_name, synced_count))
    #                 logger.info(f"表 {table_name} 成功同步 {success_count} 条记录到网盘")
                    
    #             except Exception as e:
    #                 failed_tables.append(table_name)
    #                 logger.error(f"同步表 {table_name} 失败: {str(e)}")
            
    #         # 输出汇总信息
    #         logger.info("\n========== 同步任务完成 ==========")
    #         logger.info(f"同步时间范围: {start_time} 到 {end_time}")
    #         logger.info(f"总计同步数据: {total_sync_count} 条")
    #         logger.info("\n成功同步的表:")
    #         for table, count in successful_tables:
    #             logger.info(f"  - {table}: {count} 条记录")
    #         if failed_tables:
    #             logger.info("\n同步失败的表:")
    #             for table in failed_tables:
    #                 logger.info(f"  - {table}")
    #         logger.info("====================================")
                
    #     except Exception as e:
    #         logger.error(f"同步数据到网盘时发生错误: {str(e)}")
            

    def sync_data_to_disk(self, start_time, end_time, table_names, target_path):
        """同步数据到网盘"""
        try:
            total_sync_count = 0
            successful_tables = []
            failed_tables = []
            
            for table_name in table_names:
                try:
                    # 1. 查询数据库获取数据
                    data_list = self.mapper.getDataByReceiveTime(start_time, end_time, table_name)
                    logger.info(f"从表 {table_name} 获取到 {len(data_list)} 条数据")
                    
                    # 2. 创建目标目录
                    table_path = os.path.join(target_path, table_name)
                    os.makedirs(table_path, exist_ok=True)
                    
                    # 3. 获取同步前的文件数量
                    before_count = len(os.listdir(table_path)) if os.path.exists(table_path) else 0
                    
                    # 4. 保存数据为JSON文件
                    success_count = 0
                    for data in data_list:
                        try:
                            # 确保数据中有F_DATANAME字段
                            if 'F_DATANAME' not in data:
                                logger.warning(f"数据缺少F_DATANAME字段: {data}")
                                continue

                            # 序列化数据
                            serialized_data = {key: serialize(value) for key, value in data.items()}
                            
                            # 生成文件名
                            file_name = f"{table_name}-{serialized_data['F_DATANAME']}.json"
                            file_path = os.path.join(table_path, file_name)
                            
                            # 保存文件
                            if not os.path.exists(file_path):
                                with open(file_path, 'w', encoding='utf-8') as f:
                                    json.dump(serialized_data, f, ensure_ascii=False, indent=4)
                                success_count += 1
                                logger.debug(f"成功保存文件: {file_name}")
                                
                        except Exception as e:
                            logger.error(f"处理数据记录失败: {str(e)}")
                            continue
                    
                    # 5. 获取同步后的文件数量并计算新增数量
                    after_count = len(os.listdir(table_path)) if os.path.exists(table_path) else 0
                    synced_count = after_count - before_count
                    
                    total_sync_count += synced_count
                    successful_tables.append((table_name, synced_count))
                    logger.info(f"表 {table_name} 成功同步 {success_count} 条记录到网盘")
                    
                except Exception as e:
                    failed_tables.append(table_name)
                    logger.error(f"同步表 {table_name} 失败: {str(e)}")
                    continue
            
            # 输出汇总信息
            logger.info("\n========== 同步任务完成 ==========")
            logger.info(f"同步时间范围: {start_time} 到 {end_time}")
            logger.info(f"总计同步数据: {total_sync_count} 条")
            logger.info("\n成功同步的表:")
            for table, count in successful_tables:
                logger.info(f"  - {table}: {count} 条记录")
            if failed_tables:
                logger.info("\n同步失败的表:")
                for table in failed_tables:
                    logger.info(f"  - {table}")
            logger.info("====================================")
                
        except Exception as e:
            logger.error(f"同步数据到网盘时发生错误: {str(e)}")
    
    def sync_to_external_db(self, source_path, table_names):
        """将网盘中的数据同步到外网数据库"""
        try:
            for table_name in table_names:
                # 获取对应的外网表名
                external_table = self.folder_table_mapping.get(table_name)
                if not external_table:
                    logger.warning(f"表 {table_name} 不在映射关系中，跳过处理")
                    continue

                table_path = os.path.join(source_path, table_name)
                if not os.path.exists(table_path):
                    logger.warning(f"目录不存在: {table_path}")
                    continue
                
                success_count = 0
                total_count = 0
                for file_name in os.listdir(table_path):
                    if not file_name.endswith('.json'):
                        continue
                        
                    total_count += 1
                    file_path = os.path.join(table_path, file_name)
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        # 使用映射后的表名插入数据
                        self.mapper.insert_data_into_table(external_table, data)
                        success_count += 1
                        
                    except Exception as e:
                        logger.error(f"处理文件 {file_name} 失败: {str(e)}")
                    
                logger.info(f"表 {table_name} -> {external_table}: 总计处理 {total_count} 个文件，成功 {success_count} 个")
                
        except Exception as e:
            logger.error(f"同步到外网数据库时发生错误: {str(e)}")


    def insert_json_to_db(self, source_dir, table_name):
        """将JSON文件数据插入到数据库表中"""
        try:
            if not os.path.exists(source_dir):
                logger.error(f"源目录不存在: {source_dir}")
                return

            success_count = 0
            total_count = 0
            
            for file_name in os.listdir(source_dir):
                if not file_name.endswith('.json'):
                    continue
                    
                total_count += 1
                file_path = os.path.join(source_dir, file_name)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    # 转换数据格式
                    db_data = self._transform_json_to_db_format(json_data)
                    
                    # 插入数据
                    if self.mapper.insert_data_into_table(table_name, db_data):
                        success_count += 1
                        logger.info(f"成功插入文件: {file_name}")
                    else:
                        logger.error(f"插入文件失败: {file_name}")
                        
                except Exception as e:
                    logger.error(f"处理文件 {file_name} 失败: {str(e)}")
            
            logger.info(f"处理完成: 总计 {total_count} 个文件，成功 {success_count} 个")
            
        except Exception as e:
            logger.error(f"插入JSON数据到数据库时发生错误: {str(e)}")
    
    def _transform_json_to_db_format(self, json_data):
        """转换JSON数据为数据库格式"""
        try:
            # 数值精度处理函数
            def format_float(value):  # 移除精度参数，保持原始精度
                if value is None:
                    return None
                try:
                    if isinstance(value, str):
                        return float(value)  # 直接转换为float，不进行round
                    return value
                except (ValueError, TypeError):
                    return None

            def format_int(value):
                if value is None:
                    return None
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return None

            # 基础字段映射
            db_data = {
                'F_DID': format_int(json_data.get('F_DATAID')),
                'F_DATANAME': json_data.get('F_DATANAME'),
                'F_PRODUCETIME': json_data.get('F_PRODUCETIME'),
                'F_PRODUCTID': format_int(json_data.get('F_PRODUCTID')),
                'F_PRODUCTLEVEL': json_data.get('F_PRODUCTLEVEL'),
                'F_SATELLITEID': json_data.get('F_SATELLITEID'),
                'F_SENSORID': json_data.get('F_SENSORID'),
                'F_TOPLEFTLATITUDE': format_float(json_data.get('F_DATAUPPERLEFTLAT')),
                'F_TOPLEFTLONGITUDE': format_float(json_data.get('F_DATAUPPERLEFTLONG')),
                'F_TOPRIGHTLATITUDE': format_float(json_data.get('F_DATAUPPERRIGHTLAT')),
                'F_TOPRIGHTLONGITUDE': format_float(json_data.get('F_DATAUPPERRIGHTLONG')),
                'F_BOTTOMRIGHTLATITUDE': format_float(json_data.get('F_DATALOWERRIGHTLAT')),
                'F_BOTTOMRIGHTLONGITUDE': format_float(json_data.get('F_DATALOWERRIGHTLONG')),
                'F_BOTTOMLEFTLATITUDE': format_float(json_data.get('F_DATALOWERLEFTLAT')),
                'F_BOTTOMLEFTLONGITUDE': format_float(json_data.get('F_DATALOWERLEFTLONG')),
                'F_CLOUDPERCENT': format_float(json_data.get('F_CLOUDPERCENT')),
                'F_ORBITID': format_int(json_data.get('F_ORBITID')),
                'F_SCENEID': format_int(json_data.get('F_SCENEID')),
                'F_SCENEPATH': format_int(json_data.get('F_SCENEPATH')),
                'F_SCENEROW': format_int(json_data.get('F_SCENEROW')),
                'F_RECEIVETIME': json_data.get('F_RECEIVETIME'),
                'F_IMPORTUSER': json_data.get('F_IMPORTUSER'),
                'F_IMPORTDATE': json_data.get('F_IMPORTTIME'),
                'F_DATASIZE': format_float(json_data.get('F_DATASIZE')),
                'F_LOCATION': str(format_float(json_data.get('F_IMAGEGSD'))),  # 将F_IMAGEGSD映射到F_LOCATION
                'F_PITCHSATELLITEANGLE': format_float(json_data.get('F_PITCHSATELLITEANGLE')),  # 保持原始精度
                'F_PITCHVIEWINGANGLE': format_float(json_data.get('F_PITCHVIEWINGANGLE')),
                'F_YAWSATELLITEANGLE': format_float(json_data.get('F_YAWSATELLITEANGLE')),
                'F_ROLLSATELLITEANGLE': format_float(json_data.get('F_ROLLSATELLITEANGLE')),
                'F_ROLLVIEWINGANGLE': format_float(json_data.get('F_ROLLVIEWINGANGLE'))
            }





            # 处理时间格式
            for field in ['F_PRODUCETIME', 'F_RECEIVETIME', 'F_IMPORTDATE']:
                if db_data.get(field):
                    try:
                        if 'T' in db_data[field]:
                            dt = datetime.fromisoformat(db_data[field].replace('Z', '+00:00'))
                        else:
                            try:
                                dt = datetime.strptime(db_data[field], '%Y-%m-%d %H:%M:%S.%f')
                            except ValueError:
                                dt = datetime.strptime(db_data[field], '%Y-%m-%d %H:%M:%S')
                        db_data[field] = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception as e:
                        logger.error(f"时间格式转换失败 {field}: {str(e)}")
                        db_data[field] = None

            # 过滤掉None值
            db_data = {k: v for k, v in db_data.items() if v is not None}
            
            return db_data
            
        except Exception as e:
            logger.error(f"数据转换失败: {str(e)}")
            return None

    def _create_sdo_geometry(self, coords):
        """创建SDO_GEOMETRY对象"""
        try:
            # 创建SDO_GEOMETRY对象
            sdo_geometry = self.mapper.create_sdo_geometry(coords)
            return sdo_geometry
                
        except Exception as e:
            logger.error(f"创建SDO_GEOMETRY对象失败: {str(e)}")
            return None

        