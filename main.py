import argparse
import commands

parser = argparse.ArgumentParser(prog='GeoCloud Backend Service', description='A backend service for managing geospatial data and services')

subparsers = parser.add_subparsers(title="Data Extraction", dest='subparsers')

# 内网数据提取命令
data_extraction_internal_command = subparsers.add_parser("internal", help="Data Extraction Service in Internal Machines")

# 外网数据提取命令
data_extraction_external_command = subparsers.add_parser("external", help="Data Extraction Service in External Machines")
data_extraction_external_command.add_argument('mode', 
    choices=['initial', 'daily', 'insert', 'backup', 'restore', 'rollback'], 
    help='操作模式: "initial"用于历史数据同步, "daily"用于每日同步任务, "insert"用于数据插入, "backup"用于备份, "restore"用于恢复, "rollback"用于回滚')
data_extraction_external_command.add_argument('--schema', help='数据库模式名称 (用于backup、restore和rollback)')
data_extraction_external_command.add_argument('--backup-path', help='备份文件路径 (用于restore)')
data_extraction_external_command.add_argument('--time', help='要回滚到的备份时间点 (用于rollback，格式: YYYYMMDD_HHMMSS)')
data_extraction_external_command.add_argument('--force', action='store_true', help='强制执行回滚，不进行确认 (用于rollback)')


# Web服务命令
main_service_command = subparsers.add_parser("web", help="Main Web Service")

args = parser.parse_args()
match args.subparsers:
    case 'internal':
        commands.data_extraction_internal()
    case 'external':
        commands.data_extraction_external(
            args.mode, 
            args.schema if hasattr(args, 'schema') else None,
            args.backup_path if hasattr(args, 'backup_path') else None,
            args.time if hasattr(args, 'time') else None,
            args.force if hasattr(args, 'force') else False
        )
    case 'web':
        commands.run_web()
    case _:
        print("Invalid command")
