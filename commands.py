def data_extraction_external(mode):
    """外部数据提取服务"""
    if mode == 'insert_initial':
        from src.data_extraction_service.external.main import import_initial
        import_initial()
    elif mode == 'insert_daily':
        from src.data_extraction_service.external.main import import_daily
        import_daily()
    elif mode == 'insert':
        from src.data_extraction_service.external.main import import_folder
        import_folder()
    elif mode == 'initial':
        from src.data_extraction_service.external.main import sync_data
        sync_data('initial')
    elif mode == 'daily':
        from src.data_extraction_service.external.main import sync_data
        sync_data('daily')
    elif mode == 'graph':
        from src.data_extraction_service.external.main import sync_graph_data
        sync_graph_data()
    elif mode == 'import':
        from src.data_extraction_service.external.main import import_folder_data
        import_folder_data()

def data_extraction_internal():
    # 处理内网相关功能
    pass

def run_web():
    from src.geocloudservice.web import main
    main()