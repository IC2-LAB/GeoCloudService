import sys
import argparse
from src.data_extraction_service.external.daily_sync_service import DailySyncService
import win32serviceutil
import commands

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='GeoCloud Backend Service')
    
    # 添加子命令解析器
    subparsers = parser.add_subparsers(dest='service', help='选择服务类型')
    
    # 添加 external 子命令
    external_parser = subparsers.add_parser('external')
    external_parser.add_argument('mode', 
                               choices=['initial', 'daily', 'graph', 'import', 'insert', 'insert_initial', 'insert_daily'],
                               help='运行模式')
    
    # 为 insert 模式添加文件夹参数
    external_parser.add_argument('folder_name', nargs='?', default=None,
                               help='要处理的文件夹名称 (仅 insert 模式需要)')
    
    args = parser.parse_args()
    
    if args.service == 'external':
        if args.mode == 'insert':
            if not args.folder_name:
                parser.error("insert 模式需要指定文件夹名称")
            from src.data_extraction_service.external.main import import_folder
            import_folder(args.folder_name)
        else:
            commands.data_extraction_external(args.mode)

if __name__ == '__main__':
    main()
