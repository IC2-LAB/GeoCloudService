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
import schedule
import time
import os
import json
import argparse
import shutil
import zipfile
from datetime import datetime, timedelta

from src.utils.logger import logger
from src.utils.db.oracle import create_pool
from src.data_extraction_service.external.schedule.satelliteData_process import SatelliteDataProcess
from src.data_extraction_service.external.backup_utils import (
    create_backup_dir,
    backup_table,
    create_backup_summary,
    compress_backup,
    restore_table_data,
    list_backups, 
    verify_backup  
)
from src.data_extraction_service.external.config import (
    BACKUP_PATH,
    TARGET_PATH,
    JSON_PROCESS_COUNT,
    SYNC_TABLES
)






def backup_schema(schema_name):
    """备份指定的数据库模式中的TB_META表"""
    try:
        logger.info(f"开始备份模式 {schema_name} 中的TB_META表")
        
        # 创建备份目录
        backup_dir = create_backup_dir(schema_name)
        
        # 创建数据库连接
        pool = create_pool()
        
        # 获取模式下的所有TB_META开头的表
        successful_tables = []
        failed_tables = []
        
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM all_tables 
                    WHERE owner = :schema_name
                    AND table_name LIKE 'TB_META%'
                    ORDER BY table_name
                """, {'schema_name': schema_name.upper()})
                tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            logger.warning(f"模式 {schema_name} 中未找到TB_META开头的表")
            return
        
        # 备份每个表
        for table in tables:
            try:
                count = backup_table(pool, schema_name, table, backup_dir)
                successful_tables.append((table, count))
                logger.info(f"表 {table} 备份完成: {count} 条记录")
            except Exception as e:
                failed_tables.append((table, str(e)))
                logger.error(f"备份表 {table} 失败: {str(e)}")
        
        # 创建备份摘要
        create_backup_summary(backup_dir, schema_name, successful_tables, failed_tables)
        
        # 压缩备份文件
        backup_file = compress_backup(backup_dir)
        logger.info(f"备份完成: {backup_file}")
        
        # 输出备份统计信息
        total_records = sum(count for _, count in successful_tables)
        logger.info(f"""
========== 备份统计 ==========
模式名称: {schema_name}
成功表数: {len(successful_tables)}
失败表数: {len(failed_tables)}
总记录数: {total_records}
备份文件: {backup_file}
=============================""")
        
    except Exception as e:
        logger.error(f"备份过程发生错误: {str(e)}")

def restore_schema(backup_path, schema_name):
    """从备份文件恢复数据库模式"""
    try:
        logger.info(f"开始从 {backup_path} 恢复数据到模式 {schema_name}")
        
        # 验证备份文件
        is_valid, message = verify_backup(backup_path)
        if not is_valid:
            raise Exception(f"备份文件验证失败: {message}")
        
        # 创建临时解压目录
        temp_dir = os.path.join(os.path.dirname(backup_path), 'temp_restore')
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            # 解压备份文件
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            pool = create_pool()
            successful_tables = []
            failed_tables = []
            skipped_tables = []
            
            # 处理每个JSON文件
            for file_name in os.listdir(temp_dir):
                if not file_name.endswith('.json') or file_name == 'backup_summary.txt':
                    continue
                    
                table_name = os.path.splitext(file_name)[0]
                json_path = os.path.join(temp_dir, file_name)
                
                try:
                    # 读取数据
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if not data:
                        skipped_tables.append(table_name)
                        continue
                    
                    # 处理每条记录
                    for record in data:
                        # 处理日期字段
                        date_fields = ['F_PRODUCETIME', 'F_RECEIVETIME', 'F_IMPORTDATE']
                        for field in date_fields:
                            if field in record and record[field]:
                                try:
                                    # 尝试解析不同格式的日期
                                    if isinstance(record[field], str):
                                        # 移除可能存在的时区信息
                                        date_str = record[field].split('+')[0].strip()
                                        try:
                                            # 尝试解析带毫秒的格式
                                            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
                                        except ValueError:
                                            try:
                                                # 尝试解析不带毫秒的格式
                                                dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                                            except ValueError:
                                                # 尝试解析ISO格式
                                                if 'T' in date_str:
                                                    dt = datetime.fromisoformat(date_str.replace('Z', ''))
                                                else:
                                                    raise
                                        
                                        # 统一转换为Oracle接受的格式
                                        record[field] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                                except Exception as e:
                                    logger.warning(f"日期转换失败 {field}: {record[field]} - {str(e)}")
                                    record[field] = None

                        # 处理空间信息
                        if 'F_SPATIAL_INFO' in record and record['F_SPATIAL_INFO']:
                            try:
                                if isinstance(record['F_SPATIAL_INFO'], dict) and 'coordinates' in record['F_SPATIAL_INFO']:
                                    with pool.acquire() as conn:
                                        # 创建 SDO_GEOMETRY 对象
                                        sdo_geometry = conn.gettype("MDSYS.SDO_GEOMETRY")
                                        sdo_elem_info = conn.gettype("MDSYS.SDO_ELEM_INFO_ARRAY")
                                        sdo_ordinate = conn.gettype("MDSYS.SDO_ORDINATE_ARRAY")
                                        
                                        obj = sdo_geometry.newobject()
                                        obj.SDO_GTYPE = 2003  # 2D polygon
                                        obj.SDO_SRID = 4326   # WGS84
                                        obj.SDO_ELEM_INFO = sdo_elem_info.newobject()
                                        obj.SDO_ELEM_INFO.extend([1, 1003, 1])
                                        
                                        obj.SDO_ORDINATES = sdo_ordinate.newobject()
                                        coords = record['F_SPATIAL_INFO']['coordinates']
                                        for coord in coords:
                                            obj.SDO_ORDINATES.extend([coord[0], coord[1]])
                                        
                                        record['F_SPATIAL_INFO'] = obj
                            except Exception as e:
                                logger.error(f"空间信息转换失败: {str(e)}")
                                record['F_SPATIAL_INFO'] = None
                    
                    # 批量插入数据
                    success_count, failed_count = restore_table_data(pool, schema_name, 
                                                                   table_name, data)
                    
                    if success_count > 0:
                        successful_tables.append((table_name, success_count))
                        logger.info(f"表 {table_name} 恢复完成: {success_count} 条记录")
                    if failed_count > 0:
                        logger.warning(f"表 {table_name} 有 {failed_count} 条记录恢复失败")
                        
                except Exception as e:
                    failed_tables.append((table_name, str(e)))
                    logger.error(f"恢复表 {table_name} 失败: {str(e)}")
            
            # 创建恢复报告
            report_path = os.path.join(temp_dir, 'restore_report.txt')
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(f"恢复时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"目标模式: {schema_name}\n\n")
                
                if successful_tables:
                    f.write("成功恢复的表:\n")
                    for table, count in successful_tables:
                        f.write(f"  - {table}: {count} 条记录\n")
                
                if skipped_tables:
                    f.write("\n跳过的空表:\n")
                    for table in skipped_tables:
                        f.write(f"  - {table}\n")
                
                if failed_tables:
                    f.write("\n恢复失败的表:\n")
                    for table, error in failed_tables:
                        f.write(f"  - {table}: {error}\n")
            
            logger.info(f"\n========== 恢复完成 ==========")
            logger.info(f"目标模式: {schema_name}")
            logger.info(f"成功恢复: {len(successful_tables)} 个表")
            logger.info(f"跳过表数: {len(skipped_tables)} 个表")
            logger.info(f"失败表数: {len(failed_tables)} 个表")
            logger.info(f"恢复报告: {report_path}")
            logger.info(f"=============================")
            
        finally:
            # 清理临时目录
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
            except Exception as e:
                logger.warning(f"清理临时目录失败: {str(e)}")
        
        logger.info(f"回滚完成: 模式 {schema_name} 已恢复到 {os.path.basename(backup_path).split('.')[0]}")
        
    except Exception as e:
        logger.error(f"恢复过程中发生错误: {str(e)}")
        raise

def rollback_schema(schema_name, backup_time=None, force=False):
    """回滚数据库模式到指定的备份时间点"""
    try:
        # 获取可用的备份文件
        backups = list_backups(schema_name)
        if not backups:
            logger.error(f"未找到模式 {schema_name} 的任何备份文件")
            return False

        # 如果未指定备份时间，显示可用的备份列表
        if not backup_time:
            logger.info("\n可用的备份时间点:")
            for i, backup in enumerate(backups, 1):
                logger.info(f"\n{i}. 备份时间: {backup['backup_time']}")
                logger.info(f"文件路径: {backup['file_path']}")
                logger.info("备份摘要:")
                logger.info(backup['summary'])
            return True

        # 查找匹配的备份文件
        selected_backup = None
        for backup in backups:
            if backup['backup_time'].startswith(backup_time):
                selected_backup = backup
                break

        if not selected_backup:
            logger.error(f"未找到时间点 {backup_time} 的备份文件")
            return False

        # 验证备份文件
        is_valid, message = verify_backup(selected_backup['file_path'])
        if not is_valid:
            logger.error(f"备份文件验证失败: {message}")
            return False

        # 确认回滚操作
        if not force:
            logger.warning(f"""
警告: 即将执行回滚操作
目标模式: {schema_name}
备份时间: {selected_backup['backup_time']}
备份文件: {selected_backup['file_path']}

备份摘要:
{selected_backup['summary']}

此操作将清空并重写所有TB_META表的数据。
是否继续? (y/n)""")
            
            confirm = input().lower()
            if confirm != 'y':
                logger.info("回滚操作已取消")
                return False

        # 执行回滚
        restore_schema(selected_backup['file_path'], schema_name)
        logger.info(f"回滚完成: 模式 {schema_name} 已恢复到 {backup_time}")
        return True

    except Exception as e:
        logger.error(f"回滚过程发生错误: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='数据库管理工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 添加回滚命令
    rollback_parser = subparsers.add_parser('rollback', help='回滚数据库模式到指定备份时间点')
    rollback_parser.add_argument('schema', help='要回滚的数据库模式名称')
    rollback_parser.add_argument('--time', help='要回滚到的备份时间点 (格式: YYYYMMDD_HHMMSS)')
    rollback_parser.add_argument('--force', action='store_true', help='强制执行回滚，不进行确认')
    
    args = parser.parse_args()
    
    if args.command == 'rollback':
        rollback_schema(args.schema, args.time, args.force)


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
        source_dir = r"C:\Users\1\Desktop\data_extraction\GeoCloudService\satelliteData_process\TB_META_ZY02C\test"
        table_name = "TB_META_CB04A"
        
        pool = create_pool()
        processor = SatelliteDataProcess(pool)
        processor.insert_json_to_db(source_dir, table_name)
    except Exception as e:
        logger.error(f"处理外部数据插入时发生错误: {str(e)}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='数据同步工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # 添加子命令
    parser_initial = subparsers.add_parser('initial', help='同步历史数据')
    parser_daily = subparsers.add_parser('daily', help='设置每日同步任务')
    parser_insert = subparsers.add_parser('insert', help='插入JSON数据到数据库')
    
    parser_backup = subparsers.add_parser('backup', help='备份指定的数据库模式')
    parser_backup.add_argument('schema', help='要备份的数据库模式名称')
    
    parser_restore = subparsers.add_parser('restore', help='从备份恢复数据库模式')
    parser_restore.add_argument('backup_path', help='备份文件的路径(.zip文件)')
    parser_restore.add_argument('schema', help='要恢复到的目标模式名称')
    
    args = parser.parse_args()
    
    if args.command == 'initial' or args.command == 'daily':
        sync_data(args.command)
    elif args.command == 'insert':
        process_insert()
    elif args.command == 'backup':
        backup_schema(args.schema)
    elif args.command == 'restore':
        restore_schema(args.backup_path, args.schema)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()