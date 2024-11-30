import schedule
import time 
import os

from src.data_extraction_service.external.schedule import orderProcess
from src.utils.logger import logger
from src.utils.db.oracle import create_pool
import src.config.config as config
# import src.utils.GeoDBHandler 
# import src.utils.GeoProcessor
# from src.geocloudservice.recommend import recommendData
from src.data_extraction_service.external.schedule.satelliteData_process import SatelliteDataProcess

import geopandas as gpd
from shapely.geometry import Polygon


# def main():
#     tablename = ["TB_META_ZY02C","TB_META_GF1"]
#     wkt = "POLYGON((108.0176 32.0361,108.0177 32.0361,108.0176 32.0362,108.0175 32.0361,108.0176 32.0361))"
#     pool = create_pool()
#     recommendData(tablename,wkt,None,pool)


# def main():
#     # 创建数据库连接池
#     pool = create_pool()

#     # 实例化 SatelliteDataProcess
#     processor = SatelliteDataProcess(pool)

#     # 设置时间范围和表名
#     start_time = "2015-01-22 01:29:31.000"  
#     end_time = "2015-01-22 01:29:53.000" 
#     folder_names = ["TB_META_ZY02C"]  # 假设您需要处理的文件夹名
#     table_names = ["TB_META_ZY02C", "TB_META_GF1"]  # 假设您需要处理的表名
#     # 指定目录路径和目标文件夹名
#     directory_to_scan = "C:\\Users\\1\\Desktop\\data_extraction\\GeoCloudService\\satelliteData_process" 

#     target_folder_name = "TB_META_ZY301"  # 可以根据需要修改

#     # 遍历每个表名，处理并保存数据
#     for folder_name in folder_names:
#         # processor.saveDataToJson(start_time, end_time, table_name)
#         processor.process_directory(directory_to_scan, folder_name)


# if __name__ == "__main__":
#     main()



#cmm1126修改
# import schedule
import time
from datetime import datetime, timedelta
import os
import json

from src.utils.logger import logger
from src.utils.db.oracle import create_pool
from src.data_extraction_service.external.schedule.satelliteData_process import SatelliteDataProcess

# 定义配置常量
SYNC_TABLES = ["TB_META_ZY02C", "GF1_WFV_YSDATA","GF1_YSDATA","GF1B_YSDATA","GF1C_YSDATA","GF1D_YSDATA","GF2_YSDATA","GF5_AHSIDATA","GF5_VIMSDATA","GF6_WFV_DATA","GF6_YSDATA","GF7_BWD_DATA","GF7_MUX_DATA","ZY301A_MUX_DATA","ZY301A_NAD_DATA","ZY302A_MUX_DATA","ZY302A_NAD_DATA","ZY303A_MUX_DATA","ZY303A_NAD_DATA","ZY02C_HRC_DATA","ZY02C_PMS_DATA","ZY1E_AHSI","ZY1F_AHSI","ZY1F_ISR_NSR","CB04A_VNIC","ARCHIVE_DATATRACE","ARCHIVE_METAFILETRACE","CFGMGR_FILESERVER","CSES_01_LAP","CSES_01_SCM",
               "FBDATUM_0713_142","FBDATUM_0716_142","FBDATUM_0809_162","FBDATUM_0811_162",
               "GWMD_FBDATUM_0088_2_CAT","GWMD_FBDATUM_0091_2_CAT","GWMD_FBDATUM_0101_2_CAT","GWMD_FBDATUM_0207_41_CAT","GWMD_FBDATUM_0221_61_CAT","GWMD_FBDATUM_0224_61_CAT","GWMD_FBDATUM_0227_61_CAT","GWMD_FBDATUM_0230_61_CAT","GWMD_FBDATUM_0277_2_CAT","GWMD_FBDATUM_0310_2_CAT","GWMD_FBDATUM_0353_2_CAT","GWMD_FBDATUM_0356_2_CAT","GWMD_FBDATUM_0359_2_CAT","GWMD_FBDATUM_0362_2_CAT","GWMD_FBDATUM_0367_61_CAT","GWMD_FBDATUM_0387_2_CAT","GWMD_FBDATUM_0430_61_CAT","GWMD_FBDATUM_0447_2_CAT","GWMD_FBDATUM_0450_61_CAT","GWMD_FBDATUM_0453_2_CAT","GWMD_FBDATUM_0456_2_CAT","GWMD_FBDATUM_0527_61_CAT","GWMD_FBDATUM_0530_61_CAT","GWMD_FBDATUM_0607_61_CAT","GWMD_FBDATUM_0627_122_CAT","GWMD_FBDATUM_0647_122_CAT","GWMD_FBDATUM_0650_122_CAT","GWMD_FBDATUM_0653_122_CAT","GWMD_FBDATUM_0656_122_CAT","GWMD_FBDATUM_0659_122_CAT","GWMD_FBDATUM_0662_122_CAT","GWMD_FBDATUM_0690_2_CAT",
               "H1C_CZI_L1C","H1C_OCT_L1B","H1C_OCT_L1C","H1D_CZI_L1C","H1D_OCT_L1B","H1D_OCT_L1C","H1D_OCT_L2B",
               "LT1A","LT1A_ORBIT","LT1B",
               "ZZZ_DUP_DATATRACE","ZZZ_DUP_META","ZZ_ALL_META","ZZ_OFFLINE_DATA_LIST"

               ]  # 需要同步的表名列表
# TARGET_PATH = "Z:/shareJGF/order/data/sync_folder"  # 网盘同步目标路径
TARGET_PATH = r"C:\Users\1\Desktop\data_extraction\GeoCloudService\satelliteData_process"  # 网盘同步目标路径

def sync_data(sync_type):
    """
    执行数据同步
    :param sync_type: 同步类型 ('initial' 或 'daily')
    """
    try:
        pool = create_pool()
        processor = SatelliteDataProcess(pool)
        
        if sync_type == 'initial':
            # 执行初始同步
            start_time = "2012-09-13 13:16:37.000"
            end_time = "2012-09-13 13:17:16.000"
            logger.info(f"开始初始同步，时间范围: {start_time} 到 {end_time}")
            
            # 用于统计总数据量
            total_sync_count = 0
            successful_tables = []
            failed_tables = []
            
            # 遍历配置的表，同步数据到网盘
            for table in SYNC_TABLES:
                try:
                    logger.info(f"开始同步表 {table} 的历史数据")
                    # 获取同步前的文件数量
                    table_path = os.path.join(TARGET_PATH, table)
                    before_count = len(os.listdir(table_path)) if os.path.exists(table_path) else 0
                    
                    processor.sync_data_to_disk(start_time, end_time, [table], TARGET_PATH)
                    
                    # 获取同步后的文件数量
                    after_count = len(os.listdir(table_path)) if os.path.exists(table_path) else 0
                    synced_count = after_count - before_count
                    
                    total_sync_count += synced_count
                    successful_tables.append((table, synced_count))
                    logger.info(f"表 {table} 历史数据同步完成，新增 {synced_count} 条记录")
                    
                except Exception as e:
                    failed_tables.append(table)
                    logger.error(f"同步表 {table} 时发生错误: {str(e)}")
            
            # 输出汇总信息
            logger.info("\n========== 初始同步任务完成 ==========")
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
        
        elif sync_type == 'daily':
            # 先执行一次历史数据同步
            logger.info("开始执行历史数据同步...")
            start_time = "2015-01-01 00:00:00.000"  # 设置历史起始时间
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.999")
            
            for table in SYNC_TABLES:
                try:
                    logger.info(f"同步表 {table} 的历史数据")
                    processor.sync_data_to_disk(start_time, end_time, [table], TARGET_PATH)
                except Exception as e:
                    logger.error(f"同步表 {table} 的历史数据失败: {str(e)}")
            # 设置每日同步任务
            def daily_task():
                try:
                    now = datetime.now()
                    # 设置时间范围为过去24小时
                    end_time = now.strftime("%Y-%m-%d %H:%M:%S.999")
                    start_time = (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S.000")
                    logger.info(f"开始每日同步任务: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # 同步数据到网盘
                    for table in SYNC_TABLES:
                        logger.info(f"开始同步表 {table} 的最新数据")
                        processor.sync_data_to_disk(start_time, end_time, [table], TARGET_PATH)
                        logger.info(f"表 {table} 最新数据同步到网盘完成")
                    
                    # 写入健康检查文件
                    try:
                        health_file = os.path.join(TARGET_PATH, 'health_check.txt')
                        with open(health_file, 'w') as f:
                            f.write(f"Last sync: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    except Exception as e:
                        logger.error(f"健康检查文件写入失败: {str(e)}")
                    
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


def process_insert():
    """处理JSON数据插入到外部数据库的操作"""
    try:
        # 写死的路径和表名
        source_dir = r"Y:\shareJGF\data\weixing\test"
        table_name = "TB_META_CB04A"
        
        pool = create_pool()
        processor = SatelliteDataProcess(pool)
        processor.insert_json_to_db(source_dir, table_name)
    except Exception as e:
        logger.error(f"处理外部数据插入时发生错误: {str(e)}")

def main():
    """
    主函数，提供三种运行模式：
    1. initial: 同步指定时间段的历史数据
    2. daily: 设置定时任务，每天同步新增数据
    3. insert: 将JSON文件插入到外部数据库
    """
    import argparse
    parser = argparse.ArgumentParser(description='数据同步工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # initial命令
    parser_initial = subparsers.add_parser('initial', help='同步历史数据')

    # daily命令
    parser_daily = subparsers.add_parser('daily', help='设置每日同步任务')

    # insert命令
    parser_insert = subparsers.add_parser('insert', help='插入JSON数据到数据库')
    
    args = parser.parse_args()
    
    if args.command == 'initial' or args.command == 'daily':
        sync_data(args.command)
    elif args.command == 'insert':
        process_insert()

if __name__ == "__main__":
    main()
