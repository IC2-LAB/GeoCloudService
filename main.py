import sys
import argparse
from src.data_extraction_service.external.daily_sync_service import DailySyncService
import win32serviceutil
import commands

def main():
    parser = argparse.ArgumentParser(description='GeoCloud Backend Service')
    
    # 如果是服务相关命令，直接处理
    if len(sys.argv) > 1 and sys.argv[1] in ['install', 'remove', 'start', 'stop', 'restart', 'status']:
        win32serviceutil.HandleCommandLine(DailySyncService)
        return

    # 添加子命令解析器
    subparsers = parser.add_subparsers(dest='service')
    
    # 添加 internal 子命令
    internal_parser = subparsers.add_parser('internal')
    
    # 添加 external 子命令
    external_parser = subparsers.add_parser('external')
    external_parser.add_argument('mode', choices=['initial', 'daily', 'graph', 'import'],  # 添加 'import' 选项
                               help='运行模式: initial-历史数据同步, daily-每日同步, graph-图形数据同步, import-导入测试数据')
    
    # 添加 web 子命令
    web_parser = subparsers.add_parser('web')
    
    args = parser.parse_args()
    
    if args.service == 'internal':
        commands.data_extraction_internal()
    elif args.service == 'external':
        commands.data_extraction_external(args.mode)
    elif args.service == 'web':
        commands.run_web()

if __name__ == '__main__':
    main()
