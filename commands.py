def data_extraction_external(mode):
    """外网数据提取服务"""
    from src.data_extraction_service.external.main import (
        import_initial, import_daily, import_folder_data,
        sync_graph_data, sync_data
    )
    
    if mode == 'insert_initial':
        import_initial()
    elif mode == 'insert_daily':
        import_daily()
    elif mode == 'import':
        import_folder_data()
    elif mode == 'graph':
        sync_graph_data()
    else:
        sync_data(mode)

def data_extraction_internal():
    # 处理内网相关功能
    pass

def run_web():
    from src.geocloudservice.web import main
    main()