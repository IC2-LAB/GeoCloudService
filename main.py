import argparse
import commands

parser = argparse.ArgumentParser(prog='GeoCloud Backend Service', description='A backend service for managing geospatial data and services')

subparsers = parser.add_subparsers(title="Data Extraction", dest='subparsers')

# 内网数据提取命令
data_extraction_internal_command = subparsers.add_parser("internal", help="Data Extraction Service in Internal Machines")

# 外网数据提取命令
data_extraction_external_command = subparsers.add_parser("external", help="Data Extraction Service in External Machines")
data_extraction_external_command.add_argument('mode', choices=['initial', 'daily','insert'], 
                                            help='同步模式: "initial"用于历史数据同步, "daily"用于每日同步任务')


# Web服务命令
main_service_command = subparsers.add_parser("web", help="Main Web Service")

args = parser.parse_args()
match args.subparsers:
    case 'internal':
        commands.data_extraction_internal()
    case 'external':
        commands.data_extraction_external(args.mode)
    case 'web':
        commands.run_web()
    case _:
        print("Invalid command")
