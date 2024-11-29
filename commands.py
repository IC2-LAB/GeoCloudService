from src.data_extraction_service.external.main import sync_data, process_insert

def data_extraction_internal():
    # 处理内网相关功能
    pass

def data_extraction_external(mode):
    if mode == 'initial' or mode == 'daily':
        sync_data(mode)
    elif mode == 'insert':
        process_insert()

def run_web():
    from src.geocloudservice.web import main
    main()