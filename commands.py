from src.data_extraction_service.external.main import (
    sync_data, 
    process_insert, 
    backup_schema, 
    restore_schema,
    rollback_schema  
)

def data_extraction_external(mode, schema=None, backup_path=None, time=None, force=False):
    if mode == 'initial' or mode == 'daily':
        sync_data(mode)
    elif mode == 'insert':
        process_insert()
    elif mode == 'backup' and schema:
        backup_schema(schema)
    elif mode == 'restore' and schema and backup_path:
        restore_schema(backup_path, schema)
    elif mode == 'rollback' and schema:
        rollback_schema(schema, time, force)
    else:
        print("Invalid command or missing required arguments")