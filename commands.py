def data_extraction_external(mode):
    """外网数据提取服务"""
    if mode == 'import':
        from src.data_extraction_service.external.main import import_folder_data
        import_folder_data()
    else:
        from src.data_extraction_service.external.main import sync_graph_data, sync_data
        if mode == 'graph':
            sync_graph_data()
        else:
            sync_data(mode)

def data_extraction_internal():
    # 处理内网相关功能
    pass

def run_web():
    from src.geocloudservice.web import main
    main()