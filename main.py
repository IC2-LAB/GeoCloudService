import sys
import argparse
from src.data_extraction_service.external.daily_sync_service import DailySyncService
import win32serviceutil
import commands

def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser(description='数据同步工具')
    
    # 添加子命令解析器
    subparsers = parser.add_subparsers(dest='service', help='选择服务类型')
    
    # 添加 external 子命令
    external_parser = subparsers.add_parser('external')
    external_subparsers = external_parser.add_subparsers(dest='mode', help='选择运行模式')
    
    # 添加 insert 子命令
    insert_parser = external_subparsers.add_parser('insert', help='插入数据到数据库')
    insert_subparsers = insert_parser.add_subparsers(dest='folder', help='选择要处理的文件夹')
    
    # 添加各个文件夹的子命令
    folders = [
        "GF1_WFV_YSDATA", "GF1_YSDATA", "GF1B_YSDATA", "GF1C_YSDATA", "GF1D_YSDATA",
        "GF2_YSDATA", "GF5_AHSIDATA", "GF5_VIMSDATA", "GF6_WFV_DATA", "GF6_YSDATA",
        "GF7_BWD_DATA", "GF7_MUX_DATA", "ZY301A_MUX_DATA", "ZY301A_NAD_DATA",
        "ZY302A_MUX_DATA", "ZY302A_NAD_DATA", "ZY303A_MUX_DATA", "ZY303A_NAD_DATA",
        "ZY02C_HRC_DATA", "ZY02C_PMS_DATA", "ZY1E_AHSI", "ZY1F_AHSI", "ZY1F_ISR_NSR",
        "CB04A_VNIC", "VIEW_META_BLOB"
    ]
    
    for folder in folders:
        folder_parser = insert_subparsers.add_parser(folder, help=f'处理 {folder} 文件夹')
    
    # 添加其他模式的子命令
    for mode in ['initial', 'daily', 'graph', 'import']:
        external_subparsers.add_parser(mode, help=f'{mode} 模式')
    
    args = parser.parse_args()
    
    if args.service == 'external':
        if args.mode == 'insert' and args.folder:
            from src.data_extraction_service.external.main import import_folder
            import_folder(args.folder)
        else:
            commands.data_extraction_external(args.mode)
    elif args.service == 'internal':
        commands.data_extraction_internal()
    elif args.service == 'web':
        commands.run_web()

if __name__ == '__main__':
    main()
