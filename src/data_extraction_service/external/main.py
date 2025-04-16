import schedule
import time 
import os
import json
from datetime import datetime, timedelta

from src.utils.logger import logger
from src.utils.db.oracle import create_pool
from src.data_extraction_service.external.schedule.satelliteData_process import SatelliteDataProcess

# 定义配置常量
SYNC_TABLES = [
    "TB_META_ZY02C", "TB_META_CB04A",
    "GF5_VIMSDATA","GF5_AHSIDATA","ZY1F_AHSI","ZY1F_ISR_NSR",
    "GF1_WFV_YSDATA","GF1_YSDATA","GF1B_YSDATA","GF1C_YSDATA","GF1D_YSDATA",
    "GF2_YSDATA","GF6_WFV_DATA","GF6_YSDATA","GF7_BWD_DATA","GF7_MUX_DATA",
    "ZY301A_MUX_DATA","ZY301A_NAD_DATA","ZY302A_MUX_DATA","ZY302A_NAD_DATA",
    "ZY303A_MUX_DATA","ZY303A_NAD_DATA","ZY02C_HRC_DATA","ZY02C_PMS_DATA",
    "ZY1E_AHSI","CB04A_VNIC",
]

# 图形数据同步相关常量
GRAPH_TABLES = [
    "TB_BAS_META_BLOB",  # 当前的BLOB数据表
    "YOUR_NEW_TABLE"     # 添加您想要的新表
]

# TARGET_PATH = r"C:\Users\1\Desktop\GeoCloudService\satelliteData_process"
TARGET_PATH = r"Y:\shareJGF\data\WEIXING_BUPTBACKUP"

def sync_graph_data():
    """同步图形数据到JSON文件"""
    try:
        pool = create_pool()
        processor = SatelliteDataProcess(pool)
        
        logger.info("\n" + "="*50)
        logger.info("开始执行图形数据同步任务")
        logger.info("-"*50)
        logger.info(f"需要处理的表数量: {len(GRAPH_TABLES)}")
        logger.info("-"*50)
        
        success_count = 0
        failed_tables = []
        
        for index, table in enumerate(GRAPH_TABLES, 1):
            try:
                logger.info(f"\n[{index}/{len(GRAPH_TABLES)}] 正在处理: {table}")
                
                # 创建目标目录
                table_path = os.path.join(TARGET_PATH, table)
                os.makedirs(table_path, exist_ok=True)
                
                # 获取表数据
                data_list = processor.mapper.getGraphData(table)
                logger.info(f"从表 {table} 获取到 {len(data_list)} 条数据")
                
                # 处理每条数据
                processed_count = 0
                for data in data_list:
                    try:
                        # 使用 F_DID 作为文件名
                        if 'F_DID' not in data:
                            logger.warning(f"数据缺少 F_DID 字段: {data.get('F_DATAID', 'unknown')}")
                            continue
                            
                        file_name = f"{data['F_DID']}.json"
                        file_path = os.path.join(table_path, file_name)
                        
                        if not os.path.exists(file_path):
                            with open(file_path, 'w', encoding='utf-8') as f:
                                json.dump(data, f, ensure_ascii=False, indent=4, default=str)
                            processed_count += 1
                            
                    except Exception as e:
                        logger.error(f"处理数据 DID {data.get('F_DID', 'unknown')} 失败: {str(e)}")
                
                success_count += 1
                logger.info(f"✓ {table} 处理完成: 保存 {processed_count} 条记录")
                
            except Exception as e:
                failed_tables.append((table, str(e)))
                logger.error(f"✗ {table} 处理失败: {str(e)}")
        
        # 输出汇总信息
        logger.info("\n" + "="*50)
        logger.info("图形数据同步任务完成")
        logger.info("-"*50)
        logger.info(f"成功表数量: {success_count}")
        logger.info(f"失败表数量: {len(failed_tables)}")
        
        if failed_tables:
            logger.info("\n处理失败的表:")
            for table, error in failed_tables:
                logger.info(f"   {table:<30} 错误: {error}")
        
        logger.info("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"图形数据同步任务初始化错误: {str(e)}")

def sync_data(sync_type):
    """
    执行数据同步
    :param sync_type: 同步类型 ('initial' 或 'daily')
    """
    try:
        pool = create_pool()
        processor = SatelliteDataProcess(pool)
        
        if sync_type == 'initial':
            start_time = "2002-07-20 00:00:00.000"
            end_time = "2024-12-12 09:17:16.000"#改为前一天的执行daily时间段
            logger.info(f"开始初始同步，时间范围: {start_time} 到 {end_time}")
            
            total_sync_count = 0
            total_blob_count = 0
            successful_tables = []
            failed_tables = []
            
            for index, table in enumerate(SYNC_TABLES, 1):
                try:
                    logger.info(f"\n[{index}/{len(SYNC_TABLES)}] 正在处理: {table}")
                    
                    # 创建目标目录
                    table_path = os.path.join(TARGET_PATH, table)
                    os.makedirs(table_path, exist_ok=True)
                    
                    # 获取表数据
                    data_list = processor.mapper.getDataByReceiveTime(start_time, end_time, table)
                    logger.info(f"从表 {table} 获取到 {len(data_list)} 条数据")
                    
                    synced_count = 0
                    blob_count = 0
                    
                    # 处理每条数据
                    for data in data_list:
                        try:
                            # 保存原始数据
                            file_name = f"{table}-{data['F_DATANAME']}.json"
                            file_path = os.path.join(table_path, file_name)
                            
                            if not os.path.exists(file_path):
                                with open(file_path, 'w', encoding='utf-8') as f:
                                    json.dump(data, f, ensure_ascii=False, indent=4, default=str)
                                synced_count += 1
                                logger.info(f"保存数据: {file_name}")
                            
                            # 处理BLOB数据
                            if 'F_DATAID' in data and data['F_DATAID']:  # 使用 F_DATAID
                                try:
                                    blob_data = processor.mapper.getGraphDataByDataID(data['F_DATAID'])  # 传入 F_DATAID
                                    if blob_data:
                                        blob_path = os.path.join(TARGET_PATH, "TB_BAS_META_BLOB")
                                        os.makedirs(blob_path, exist_ok=True)
                                        
                                        blob_file = f"{data['F_DATAID']}.json"  # 使用 F_DATAID 作为文件名
                                        blob_file_path = os.path.join(blob_path, blob_file)
                                        
                                        if not os.path.exists(blob_file_path):
                                            with open(blob_file_path, 'w', encoding='utf-8') as f:
                                                json.dump(blob_data, f, ensure_ascii=False, indent=4, default=str)
                                            blob_count += 1
                                            logger.info(f"保存BLOB数据: {blob_file}")
                                        else:
                                            logger.warning(f"未找到DataID={data['F_DATAID']}的图像数据")
                                    else:
                                        logger.warning(f"未找到DataID={data['F_DATAID']}的图像数据")
                                except Exception as e:
                                    logger.error(f"处理DataID={data['F_DATAID']}的图像数据时出错: {str(e)}")
                            else:
                                logger.debug(f"数据没有F_DATAID字段或F_DATAID为空: {data.get('F_DATANAME')}")
                            
                        except Exception as e:
                            logger.error(f"处理数据失败: {str(e)}")
                    
                    total_sync_count += synced_count
                    total_blob_count += blob_count
                    successful_tables.append((table, synced_count, blob_count))
                    logger.info(f"✓ {table} 同步完成: 新增 {synced_count} 条记录, {blob_count} 条BLOB数据")
                    
                except Exception as e:
                    failed_tables.append((table, str(e)))
                    logger.error(f"✗ {table} 同步失败: {str(e)}")
            
            # 输出汇总信息
            logger.info("\n" + "="*50)
            logger.info("初始同步任务完成")
            logger.info("-"*50)
            logger.info(f"总计同步数据: {total_sync_count} 条")
            logger.info(f"总计BLOB数据: {total_blob_count} 条")
            logger.info(f"成功表数量: {len(successful_tables)}")
            logger.info(f"失败表数量: {len(failed_tables)}")
            
            if successful_tables:
                logger.info("\n成功同步的表:")
                for table, count, blob_count in successful_tables:
                    logger.info(f"  ✓ {table:<30} {count:>6} 条记录, {blob_count:>6} 条BLOB数据")
            
            if failed_tables:
                logger.info("\n同步失败的表:")
                for table, error in failed_tables:
                    logger.info(f"  ✗ {table:<30} 错误: {error}")
            
            logger.info("="*50 + "\n")
            
        elif sync_type == 'daily':
            def daily_task():
                try:
                    now = datetime.now()
                    # 设置时间范围为过去24小时
                    end_time = now.strftime("%Y-%m-%d %H:%M:%S.999")
                    start_time = (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S.000")
                    logger.info(f"\n开始每日同步，时间范围: {start_time} 到 {end_time}")
                    
                    total_sync_count = 0
                    total_blob_count = 0
                    successful_tables = []
                    failed_tables = []
                    
                    # 同步数据到网盘
                    for index, table in enumerate(SYNC_TABLES, 1):
                        try:
                            logger.info(f"\n[{index}/{len(SYNC_TABLES)}] 正在处理: {table}")
                            
                            # 创建目标目录
                            table_path = os.path.join(TARGET_PATH, table)
                            os.makedirs(table_path, exist_ok=True)
                            
                            # 获取表数据
                            data_list = processor.mapper.getDataByReceiveTime(start_time, end_time, table)
                            logger.info(f"从表 {table} 获取到 {len(data_list)} 条数据")
                            
                            synced_count = 0
                            blob_count = 0
                            
                            # 处理每条数据
                            for data in data_list:
                                try:
                                    # 保存原始数据
                                    file_name = f"{table}-{data['F_DATANAME']}.json"
                                    file_path = os.path.join(table_path, file_name)
                                    if not os.path.exists(file_path):
                                        with open(file_path, 'w', encoding='utf-8') as f:
                                            json.dump(data, f, ensure_ascii=False, indent=4, default=str)
                                        synced_count += 1
                                        logger.info(f"保存数据: {file_name}")
                                    
                                    # 处理BLOB数据（如果有）
                                    if 'F_DATAID' in data and data['F_DATAID']:  # 使用 F_DATAID
                                        try:
                                            blob_data = processor.mapper.getGraphDataByDataID(data['F_DATAID'])  # 传入 F_DATAID
                                            if blob_data:
                                                blob_path = os.path.join(TARGET_PATH, "TB_BAS_META_BLOB")
                                                os.makedirs(blob_path, exist_ok=True)
                                                
                                                blob_file = f"{data['F_DATAID']}.json"  # 使用 F_DATAID 作为文件名
                                                blob_file_path = os.path.join(blob_path, blob_file)
                                                
                                                if not os.path.exists(blob_file_path):
                                                    with open(blob_file_path, 'w', encoding='utf-8') as f:
                                                        json.dump(blob_data, f, ensure_ascii=False, indent=4, default=str)
                                                    blob_count += 1
                                                    logger.info(f"保存BLOB数据: {blob_file}")
                                                else:
                                                    logger.warning(f"未找到DataID={data['F_DATAID']}的图像数据")
                                            else:
                                                logger.warning(f"未找到DataID={data['F_DATAID']}的图像数据")
                                        except Exception as e:
                                            logger.error(f"处理DataID={data['F_DATAID']}的图像数据时出错: {str(e)}")
                                    else:
                                        logger.debug(f"数据没有F_DATAID字段或F_DATAID为空: {data.get('F_DATANAME')}")
                                    
                                except Exception as e:
                                    logger.error(f"处理数据失败: {str(e)}")
                            
                            total_sync_count += synced_count
                            total_blob_count += blob_count
                            successful_tables.append((table, synced_count, blob_count))
                            logger.info(f"✓ {table} 同步完成: 新增 {synced_count} 条记录, {blob_count} 条BLOB数据")
                            
                        except Exception as e:
                            failed_tables.append((table, str(e)))
                            logger.error(f"✗ {table} 同步失败: {str(e)}")
                    
                    # 输出汇总信息
                    logger.info("\n" + "="*50)
                    logger.info("每日同步任务完成")
                    logger.info("-"*50)
                    logger.info(f"总计同步数据: {total_sync_count} 条")
                    logger.info(f"总计BLOB数据: {total_blob_count} 条")
                    logger.info(f"成功表数量: {len(successful_tables)}")
                    logger.info(f"失败表数量: {len(failed_tables)}")
                    
                    if successful_tables:
                        logger.info("\n成功同步的表:")
                        for table, count, blob_count in successful_tables:
                            logger.info(f"  ✓ {table:<30} {count:>6} 条记录, {blob_count:>6} 条BLOB数据")
                    
                    if failed_tables:
                        logger.info("\n同步失败的表:")
                        for table, error in failed_tables:
                            logger.info(f"  ✗ {table:<30} 错误: {error}")
                    
                    logger.info("="*50 + "\n")
                    
                    # 写入健康检查文件
                    try:
                        health_file = os.path.join(TARGET_PATH, 'health_check.txt')
                        with open(health_file, 'w') as f:
                            f.write(f"Last sync: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                    except Exception as e:
                        logger.error(f"健康检查文件写入失败: {str(e)}")
                        
                    # 在所有文件夹处理完成后，更新最新数据时间
                    try:
                        update_last_time_sql = """
                        MERGE INTO TB_LASTDATE t
                        USING (
                            WITH LatestTime AS (
                                SELECT MAX(F_RECEIVETIME) AS latest_recivetime
                                FROM (
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_GF1 WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_GF1B WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_GF1BCD WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_GF1C WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_GF1D WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_GF2 WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_GF5 WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_GF6 WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_GF7 WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_ZY02C WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_ZY1E WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_ZY1F WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_ZY301 WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_ZY302 WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_ZY303 WHERE F_RECEIVETIME IS NOT NULL
                                    UNION ALL  
                                    SELECT F_RECEIVETIME
                                    FROM TB_META_CB04A WHERE F_RECEIVETIME IS NOT NULL 
                                )
                            )
                            SELECT TO_CHAR(latest_recivetime, 'YYYY-MM-DD HH24:MI:SS') AS latest_recivetime,
                                   (SELECT MIN(ROWID) FROM TB_LASTDATE) AS target_rowid
                            FROM LatestTime
                        ) lt
                        ON (t.ROWID = lt.target_rowid)
                        WHEN MATCHED THEN
                            UPDATE SET t.F_LASTTIME = lt.latest_recivetime
                        """
                        
                        with pool.acquire() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute(update_last_time_sql)
                                conn.commit()
                                logger.info("✓ 成功更新最新数据时间")
                                
                    except Exception as e:
                        logger.error(f"更新最新数据时间失败: {str(e)}")
                        
                except Exception as e:
                    logger.error(f"每日同步任务执行错误: {str(e)}")

            # 设置每天凌晨2点执行
            schedule.every().day.at("02:00").do(daily_task)
            logger.info("已设置每天凌晨2点同步任务")
            
            # 立即执行一次
            logger.info("执行首次同步...")
            daily_task()
            
            # 持续运行定时任务
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次是否需要执行任务
                
    except Exception as e:
        logger.error(f"同步任务初始化错误: {str(e)}")

def import_graph_data():
    """从JSON文件导入图形数据到数据库"""
    try:
        pool = create_pool()
        processor = SatelliteDataProcess(pool)
        
        logger.info("\n" + "="*50)
        logger.info("开始导入图形数据到数据库")
        logger.info("-"*50)
        
        # 检查BLOB数据目录
        blob_path = os.path.join(TARGET_PATH, "TB_BAS_META_BLOB")
        if not os.path.exists(blob_path):
            logger.error(f"BLOB数据目录不存在: {blob_path}")
            return
            
        # 获取所有JSON文件
        json_files = [f for f in os.listdir(blob_path) if f.endswith('.json')]
        logger.info(f"找到 {len(json_files)} 个JSON文件")
        
        success_count = 0
        failed_count = 0
        
        # 处理每个文件
        for file_name in json_files:
            try:
                file_path = os.path.join(blob_path, file_name)
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if processor.mapper.insertGraphData(data):
                    success_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"处理文件失败 {file_name}: {str(e)}")
                failed_count += 1
        
        # 输出汇总信息
        logger.info("\n" + "="*50)
        logger.info("图形数据导入完成")
        logger.info("-"*50)
        logger.info(f"成功导入: {success_count} 条")
        logger.info(f"失败数量: {failed_count} 条")
        logger.info("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"导入任务初始化错误: {str(e)}")

def import_folder_data():
    """从文件夹导入数据到数据库"""
    try:
        pool = create_pool()
        processor = SatelliteDataProcess(pool)
        
        # 定义文件夹和表的映射关系
        folder_table_mapping = {
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
        
        # 基础数据目录
        # base_path = "test_insert"
        
        logger.info("\n" + "="*50)
        logger.info("开始导入文件夹数据")
        logger.info("-"*50)
        
        total_success = 0
        total_failed = 0
        
        # 遍历基础目录下的所有文件夹
        for folder_name in os.listdir(base_path):
            folder_path = os.path.join(base_path, folder_name)
            if not os.path.isdir(folder_path):
                continue
                
            # 检查是否是卫星数据文件夹
            if folder_name in folder_table_mapping:
                table_name = folder_table_mapping[folder_name]
                logger.info(f"\n处理文件夹: {folder_name} -> {table_name}")
                
                success_count = 0
                failed_count = 0
                
                # 处理文件夹中的所有JSON文件
                for file_name in os.listdir(folder_path):
                    if not file_name.endswith('.json'):
                        continue
                        
                    file_path = os.path.join(folder_path, file_name)
                    try:
                        # 读取JSON文件
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        # 插入数据到对应的表
                        if processor.mapper.insert_satellite_data(table_name, data):
                            success_count += 1
                            logger.info(f"✓ 成功导入: {file_name}")
                        else:
                            failed_count += 1
                            logger.error(f"✗ 导入失败: {file_name}")
                            
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"处理文件失败 {file_name}: {str(e)}")
                
                total_success += success_count
                total_failed += failed_count
                logger.info(f"文件夹 {folder_name} 处理完成: 成功 {success_count} 个, 失败 {failed_count} 个")
            
            # 检查是否是图像数据文件夹
            elif folder_name == "VIEW_META_BLOB":
                logger.info(f"\n处理图像数据文件夹: {folder_name}")
                success_count = 0
                failed_count = 0
                
                for file_name in os.listdir(folder_path):
                    if not file_name.endswith('.json'):
                        continue
                        
                    file_path = os.path.join(folder_path, file_name)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        if processor.mapper.insertGraphData(data):
                            success_count += 1
                            logger.info(f"✓ 成功导入图像数据: {file_name}")
                        else:
                            failed_count += 1
                            logger.error(f"✗ 导入图像数据失败: {file_name}")
                            
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"处理图像数据文件失败 {file_name}: {str(e)}")
                
                total_success += success_count
                total_failed += failed_count
                logger.info(f"图像数据处理完成: 成功 {success_count} 个, 失败 {failed_count} 个")
        
        # 输出总结信息
        logger.info("\n" + "="*50)
        logger.info("导入任务完成")
        logger.info("-"*50)
        logger.info(f"总计成功: {total_success} 个")
        logger.info(f"总计失败: {total_failed} 个")
        logger.info("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"导入任务初始化错误: {str(e)}")

def import_initial():
    """初始导入所有文件夹中的数据"""
    try:
        # 获取当前日期时间作为任务标识
        task_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info("\n" + "="*50)
        logger.info(f"开始初始导入任务 - {task_start_time}")
        logger.info("-"*50)
        
        pool = create_pool()
        processor = SatelliteDataProcess(pool)
        
        # 定义文件夹和表的映射关系
        folder_table_mapping = {
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
        
        # 使用 test_insert 目录
        base_path = "test_insert"
        
        total_success = 0
        total_failed = 0
        successful_folders = []
        failed_folders = []
        
        # 遍历所有文件夹
        for folder_name in os.listdir(base_path):
            folder_path = os.path.join(base_path, folder_name)
            if not os.path.isdir(folder_path):
                continue
                
            # 处理卫星数据文件夹
            if folder_name in folder_table_mapping:
                table_name = folder_table_mapping[folder_name]
                logger.info(f"\n处理文件夹: {folder_name} -> {table_name}")
                
                success_count = 0
                failed_count = 0
                
                # 处理文件夹中的所有JSON文件
                for file_name in os.listdir(folder_path):
                    if not file_name.endswith('.json'):
                        continue
                        
                    file_path = os.path.join(folder_path, file_name)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        if processor.mapper.insert_satellite_data(table_name, data):
                            success_count += 1
                            logger.info(f"✓ 成功导入: {file_name}")
                        else:
                            failed_count += 1
                            logger.error(f"✗ 导入失败: {file_name}")
                            
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"处理文件失败 {file_name}: {str(e)}")
                
                total_success += success_count
                total_failed += failed_count
                
                if failed_count == 0:
                    successful_folders.append((folder_name, success_count))
                else:
                    failed_folders.append((folder_name, success_count, failed_count))
                
                logger.info(f"文件夹 {folder_name} 处理完成: 成功 {success_count} 个, 失败 {failed_count} 个")
            
            # 处理图像数据文件夹
            elif folder_name == "VIEW_META_BLOB":
                logger.info(f"\n处理图像数据文件夹: {folder_name}")
                success_count = 0
                failed_count = 0
                
                for file_name in os.listdir(folder_path):
                    if not file_name.endswith('.json'):
                        continue
                        
                    file_path = os.path.join(folder_path, file_name)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        if processor.mapper.insertGraphData(data):
                            success_count += 1
                            logger.info(f"✓ 成功导入图像数据: {file_name}")
                        else:
                            failed_count += 1
                            logger.error(f"✗ 导入图像数据失败: {file_name}")
                            
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"处理图像数据文件失败 {file_name}: {str(e)}")
                
                total_success += success_count
                total_failed += failed_count
                
                if failed_count == 0:
                    successful_folders.append((folder_name, success_count))
                else:
                    failed_folders.append((folder_name, success_count, failed_count))
                
                logger.info(f"图像数据处理完成: 成功 {success_count} 个, 失败 {failed_count} 个")
        
        # 输出总结信息
        task_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info("\n" + "="*50)
        logger.info(f"初始导入任务完成 - {task_end_time}")
        logger.info("-"*50)
        logger.info(f"总计成功: {total_success} 个")
        logger.info(f"总计失败: {total_failed} 个")
        
        if successful_folders:
            logger.info("\n完全成功的文件夹:")
            for folder, count in successful_folders:
                logger.info(f"  ✓ {folder:<30} {count:>6} 条记录")
        
        if failed_folders:
            logger.info("\n部分失败的文件夹:")
            for folder, success, failed in failed_folders:
                logger.info(f"  ✗ {folder:<30} 成功 {success:>6} 条, 失败 {failed:>6} 条")
        
        logger.info("="*50 + "\n")
        
        # 更新健康检查文件
        health_file = os.path.join("logs", 'import_health.txt')
        with open(health_file, 'w', encoding='utf-8') as f:
            f.write(f"Last initial import: {task_end_time}\n")
            f.write(f"Total success: {total_success}\n")
            f.write(f"Total failed: {total_failed}\n")
        
    except Exception as e:
        logger.error(f"初始导入任务错误: {str(e)}")

def import_daily():
    """设置每日导入任务，每天凌晨4点执行，检查前24小时的数据"""
    def daily_task():
        try:
            pool = create_pool()  
            processor = SatelliteDataProcess(pool)
            now = datetime.now()
            logger.info(f"\n{'='*50}")
            logger.info(f"开始执行每日导入任务: {now.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("-"*50)
            
            # 修改为检查前24小时的数据
            end_time = now
            start_time = now - timedelta(days=1)  # 改为1天前
            
            logger.info(f"检查时间范围: {start_time} 到 {end_time}")
            
            # 定义需要处理的所有文件夹
            folders = [
                "GF1_WFV_YSDATA", "GF1_YSDATA", "GF1B_YSDATA", "GF1C_YSDATA", "GF1D_YSDATA",
                "GF2_YSDATA", "GF5_AHSIDATA", "GF5_VIMSDATA", "GF6_WFV_DATA", "GF6_YSDATA",
                "GF7_BWD_DATA", "GF7_MUX_DATA", "ZY301A_MUX_DATA", "ZY301A_NAD_DATA",
                "ZY302A_MUX_DATA", "ZY302A_NAD_DATA", "ZY303A_MUX_DATA", "ZY303A_NAD_DATA",
                "ZY02C_HRC_DATA", "ZY02C_PMS_DATA", "ZY1E_AHSI", "ZY1F_AHSI", "ZY1F_ISR_NSR",
                "CB04A_VNIC", "VIEW_META_BLOB"
            ]
            
            total_success = 0
            total_failed = 0
            successful_folders = []
            failed_folders = []
            
            # 处理每个文件夹
            for folder_name in folders:
                try:
                    logger.info(f"\n处理文件夹: {folder_name}")
                    
                    # 调用 import_folder 函数处理单个文件夹
                    result = import_folder(folder_name, start_time, end_time)
                    
                    if isinstance(result, tuple):
                        success_count, failed_count = result
                        total_success += success_count
                        total_failed += failed_count
                        
                        if failed_count == 0:
                            successful_folders.append((folder_name, success_count))
                        else:
                            failed_folders.append((folder_name, success_count, failed_count))
                    
                except Exception as e:
                    logger.error(f"处理文件夹 {folder_name} 时出错: {str(e)}")
                    failed_folders.append((folder_name, 0, 0))
            
            # 输出总结信息
            task_end_time = datetime.now()
            logger.info(f"\n{'='*50}")
            logger.info(f"每日导入任务完成 - {task_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("-"*50)
            logger.info(f"总计成功: {total_success} 个")
            logger.info(f"总计失败: {total_failed} 个")
            
            if successful_folders:
                logger.info("\n完全成功的文件夹:")
                for folder, count in successful_folders:
                    logger.info(f"  ✓ {folder:<30} {count:>6} 条记录")
            
            if failed_folders:
                logger.info("\n部分失败的文件夹:")
                for folder, success, failed in failed_folders:
                    logger.info(f"  ✗ {folder:<30} 成功 {success:>6} 条, 失败 {failed:>6} 条")
            
            logger.info("="*50)
            
            # 更新健康检查文件
            health_file = os.path.join("logs", 'daily_import_health.txt')
            with open(health_file, 'w', encoding='utf-8') as f:
                f.write(f"Last daily import: {task_end_time}\n")
                f.write(f"Time range: {start_time} to {end_time}\n")
                f.write(f"Total success: {total_success}\n")
                f.write(f"Total failed: {total_failed}\n")
                
            # 在所有文件夹处理完成后，更新最新数据时间
            try:
                update_last_time_sql = """
                    MERGE INTO TB_LASTDATE t
                    USING (
                    WITH LatestTime AS (
                        SELECT MAX(F_RECEIVETIME) AS latest_recivetime
                        FROM (
                        SELECT F_RECEIVETIME
                        FROM TB_META_GF1 WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL
                        SELECT F_RECEIVETIME
                        FROM TB_META_GF1B WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_GF1BCD WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_GF1C WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_GF1D WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_GF2 WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_GF5 WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_GF6 WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_GF7 WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_ZY02C WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_ZY1E WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_ZY1F WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_ZY301 WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_ZY302 WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_ZY303 WHERE F_RECEIVETIME IS NOT NULL
                        UNION ALL  
                        SELECT F_RECEIVETIME
                        FROM TB_META_CB04A WHERE F_RECEIVETIME IS NOT NULL 
                        )
                    )
                    SELECT TO_CHAR(latest_recivetime, 'YYYY-MM-DD HH24:MI:SS') AS latest_recivetime,
                            (SELECT MIN(ROWID) FROM TB_LASTDATE) AS target_rowid
                    FROM LatestTime
                    ) lt
                    ON (t.ROWID = lt.target_rowid)
                    WHEN MATCHED THEN
                    UPDATE SET t.F_LASTTIME = lt.latest_recivetime

                """
                
                with pool.acquire() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(update_last_time_sql)
                        conn.commit()
                        logger.info("✓ 成功更新最新数据时间")
                        # 修改以下三表的F_SATELLITEID字段：TB_META_GF7、TB_META_ZY302、TB_META_ZY303
                        update_sqls = [
                            """
                            UPDATE TB_META_GF7 tmg
                            SET tmg.F_SATELLITEID = 'GF7'
                            WHERE tmg.F_SATELLITEID = 'GF7-1'
                            """,
                            """
                            UPDATE TB_META_ZY302 tmz
                            SET tmz.F_SATELLITEID = 'ZY302'
                            WHERE tmz.F_SATELLITEID = 'ZY3-2'
                            """,
                            """
                            UPDATE TB_META_ZY303 tmz
                            SET tmz.F_SATELLITEID = 'ZY303'
                            WHERE tmz.F_SATELLITEID = 'ZY3-3'
                            """
                        ]
                        
                        for sql in update_sqls:
                            cursor.execute(sql)
                            logger.info("✓ 成功修改以下三表的F_SATELLITEID字段：TB_META_GF7、TB_META_ZY302、TB_META_ZY303")

                        conn.commit()
                        
            except Exception as e:
                logger.error(f"更新最新数据时间失败: {str(e)}")
                
        except Exception as e:
            logger.error(f"每日导入任务错误: {str(e)}")
    
    # 修改为凌晨4点执行
    schedule.every().day.at("04:00").do(daily_task)
    
    logger.info("每日导入服务已启动，将在每天 04:00 执行")
    logger.info("将处理所有文件夹的前24小时新数据")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

def import_folder(folder_name, start_time=None, end_time=None):
    """处理单个文件夹的数据，可选指定时间范围"""
    try:
        # 获取当前日期时间作为任务标识
        task_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info("\n" + "="*50)
        logger.info(f"开始处理文件夹 {folder_name} - {task_start_time}")
        logger.info("-"*50)
        
        pool = create_pool()
        processor = SatelliteDataProcess(pool)
        
        # 定义文件夹和表的映射关系
        folder_table_mapping = {
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
        
        # 使用 Y 盘路径
        base_path = r"Y:\shareJGF\data\WEIXING_BUPTBACKUP"
        # base_path = "test_insert"
        folder_path = os.path.join(base_path, folder_name)
        
        if not os.path.exists(folder_path):
            logger.error(f"文件夹不存在: {folder_path}")
            return
        
        success_count = 0
        failed_count = 0
        
        # 处理卫星数据文件夹
        if folder_name in folder_table_mapping:
            table_name = folder_table_mapping[folder_name]
            logger.info(f"处理卫星数据: {folder_name} -> {table_name}")
            
            # 处理文件夹中的所有JSON文件
            for file_name in os.listdir(folder_path):
                if not file_name.endswith('.json'):
                    continue
                    
                file_path = os.path.join(folder_path, file_name)
                
                # 检查文件修改时间是否在指定范围内
                if start_time and end_time:
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if not (start_time <= file_mtime <= end_time):
                        continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if processor.mapper.insert_satellite_data(table_name, data):
                        success_count += 1
                        logger.info(f"✓ 成功导入: {file_name}")
                    else:
                        failed_count += 1
                        logger.error(f"✗ 导入失败: {file_name}")
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"处理文件失败 {file_name}: {str(e)}")
        
        # 处理图像数据文件夹
        elif folder_name == "VIEW_META_BLOB":
            logger.info("处理图像数据")
            
            for file_name in os.listdir(folder_path):
                if not file_name.endswith('.json'):
                    continue
                    
                file_path = os.path.join(folder_path, file_name)
                
                # 检查文件修改时间是否在指定范围内
                if start_time and end_time:
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if not (start_time <= file_mtime <= end_time):
                        continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if processor.mapper.insertGraphData(data):
                        success_count += 1
                        logger.info(f"✓ 成功导入图像数据: {file_name}")
                    else:
                        failed_count += 1
                        logger.error(f"✗ 导入图像数据失败: {file_name}")
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"处理图像数据文件失败 {file_name}: {str(e)}")
        
        # 输出总结信息
        task_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info("\n" + "="*50)
        logger.info(f"文件夹 {folder_name} 处理完成 - {task_end_time}")
        logger.info("-"*50)
        logger.info(f"成功: {success_count} 个")
        logger.info(f"失败: {failed_count} 个")
        logger.info("="*50 + "\n")
        
        # 更新健康检查文件
        health_file = os.path.join("logs", f'import_{folder_name}_health.txt')
        with open(health_file, 'w', encoding='utf-8') as f:
            f.write(f"Last import: {task_end_time}\n")
            f.write(f"Success: {success_count}\n")
            f.write(f"Failed: {failed_count}\n")
        
        # 文件夹处理完成后，填充 F_DATAID
        if success_count > 0:  # 只有在成功插入数据后才执行
            try:
                fill_dataid_sql = f"""
                    DECLARE
                        v_max_value NUMBER;
                    BEGIN
                        SELECT NVL(MAX(F_DATAID), 0) INTO v_max_value 
                        FROM JGF_GXFW.{table_name};

                        FOR rec IN (
                            SELECT ROWID AS row_id 
                            FROM JGF_GXFW.{table_name} 
                            WHERE F_DATAID IS NULL 
                            ORDER BY F_RECEIVETIME ASC
                        )
                        LOOP
                            v_max_value := v_max_value + 1;
                            UPDATE JGF_GXFW.{table_name} 
                            SET F_DATAID = v_max_value 
                            WHERE ROWID = rec.row_id;
                        END LOOP;
                    END;
                """
                    
                with pool.acquire() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(fill_dataid_sql)
                        conn.commit()
                        logger.info(f"✓ 成功填充 {table_name} 表的 F_DATAID 字段")
                            
            except Exception as e:
                logger.error(f"填充 {table_name} 表的 F_DATAID 字段时出错: {str(e)}")
                failed_count += 1
        
        return success_count, failed_count
        
    except Exception as e:
        logger.error(f"处理文件夹 {folder_name} 错误: {str(e)}")
        return 0, 0

def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser(description='数据同步工具')
    parser.add_argument('mode', choices=['initial', 'daily', 'graph', 'import', 'insert_initial', 'insert_daily'], 
                       help='运行模式: initial-历史数据同步, daily-每日同步, graph-图形数据同步, '
                            'import-导入数据到数据库, insert_initial-初始插入所有数据, insert_daily-每日定时插入新数据')
    
    args = parser.parse_args()
    
    if args.mode == 'insert_initial':
        import_initial()
    elif args.mode == 'insert_daily':
        import_daily()
    elif args.mode in ['initial', 'daily']:
        sync_data(args.mode)
    elif args.mode == 'graph':
        sync_graph_data()
    elif args.mode == 'import':
        import_folder_data()

if __name__ == "__main__":
    main() 