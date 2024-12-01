import os
import json
import shutil
import zipfile
from datetime import datetime
from src.utils.logger import logger
from src.data_extraction_service.external.config import BACKUP_PATH
import oracledb
from datetime import datetime
from src.utils.db.mapper import Mapper


def restore_table_data(pool, schema_name, table_name, data):
    """恢复表数据"""
    try:
        success_count = 0
        failed_count = 0
        mapper = Mapper(pool)

        # 构建完整的表名
        full_table_name = f"{schema_name}.{table_name}"

        # 逐条插入数据
        for record in data:
            try:
                # 使用 Mapper 类的插入方法
                if mapper.insert_data_into_table(full_table_name, record):
                    success_count += 1
                else:
                    failed_count += 1
                    logger.error(f"插入记录失败: {record}")
            except Exception as e:
                failed_count += 1
                logger.error(f"插入记录时发生错误: {str(e)}")

        return success_count, failed_count

    except Exception as e:
        logger.error(f"恢复表 {table_name} 数据时发生错误: {str(e)}")
        return 0, len(data)


def restore_schema(backup_path, schema_name):
    try:
        logger.info(f"开始从 {backup_path} 恢复数据到模式 {schema_name}")
        
        # 验证备份文件
        is_valid, message = verify_backup(backup_path)
        if not is_valid:
            raise Exception(f"备份文件验证失败: {message}")
        
        # 创建临时解压目录
        temp_dir = os.path.join(os.path.dirname(backup_path), 'temp_restore')
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            # 解压备份文件
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            pool = create_pool()
            successful_tables = []
            failed_tables = []
            skipped_tables = []
            
            # 处理每个JSON文件
            for file_name in os.listdir(temp_dir):
                if not file_name.endswith('.json') or file_name == 'backup_summary.txt':
                    continue
                    
                table_name = os.path.splitext(file_name)[0]
                json_path = os.path.join(temp_dir, file_name)
                
                try:
                    # 读取数据
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if not data:
                        skipped_tables.append(table_name)
                        continue
                    
                    # 处理每条记录
                    for record in data:
                        # 处理日期字段
                        for field in ['F_PRODUCETIME', 'F_RECEIVETIME', 'F_IMPORTDATE']:
                            if field in record and record[field]:
                                try:
                                    # 统一日期格式
                                    if isinstance(record[field], str):
                                        # 移除可能存在的时区信息
                                        date_str = record[field].split('+')[0].split('-')[0].strip()
                                        try:
                                            # 尝试解析带毫秒的格式
                                            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
                                        except ValueError:
                                            try:
                                                # 尝试解析不带毫秒的格式
                                                dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                                            except ValueError:
                                                # 尝试解析ISO格式
                                                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                        
                                        # 统一转换为Oracle接受的格式
                                        record[field] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                                except Exception as e:
                                    logger.warning(f"日期转换失败 {field}: {record[field]} - {str(e)}")
                                    record[field] = None
                        
                        # 处理空间信息
                        if 'F_SPATIAL_INFO' in record and record['F_SPATIAL_INFO']:
                            try:
                                with pool.acquire() as conn:
                                    record['F_SPATIAL_INFO'] = create_sdo_geometry(conn, 
                                        record['F_SPATIAL_INFO']['coordinates'])
                            except Exception as e:
                                logger.error(f"空间信息转换失败: {str(e)}")
                                record['F_SPATIAL_INFO'] = None
                    
                    # 批量插入数据
                    success_count, failed_count = restore_table_data(pool, schema_name, 
                        table_name, data)
                    
                    if success_count > 0:
                        successful_tables.append((table_name, success_count))
                        logger.info(f"表 {table_name} 恢复完成: {success_count} 条记录")
                    if failed_count > 0:
                        logger.warning(f"表 {table_name} 有 {failed_count} 条记录恢复失败")
                        
                except Exception as e:
                    failed_tables.append((table_name, str(e)))
                    logger.error(f"恢复表 {table_name} 失败: {str(e)}")
            
            # 创建恢复报告...（保持原有代码）
            
        finally:
            # 清理临时文件
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
    except Exception as e:
        logger.error(f"恢复过程发生错误: {str(e)}")
        raise




def format_date_string(date_str):
    """格式化日期字符串，确保保留毫秒"""
    try:
        if not date_str:
            return None
            
        if isinstance(date_str, str):
            # 处理ISO格式
            if 'T' in date_str:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                # 处理已有毫秒的格式
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    # 处理无毫秒的格式
                    try:
                        dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError as e:
                        logger.error(f"无法解析日期格式: {date_str} - {str(e)}")
                        return None
            
            # 统一返回带3位毫秒的格式
            return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
        elif isinstance(date_str, datetime):
            return date_str.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
        return None
    except Exception as e:
        logger.error(f"日期格式化失败: {date_str} - {str(e)}")
        return None

def list_backups(schema_name):
    """列出指定模式的所有备份文件"""
    backup_files = []
    try:
        for file in os.listdir(BACKUP_PATH):
            if file.startswith(f"{schema_name}_") and file.endswith(".zip"):
                backup_time = file.replace(f"{schema_name}_", "").replace(".zip", "")
                file_path = os.path.join(BACKUP_PATH, file)
                # 获取文件信息
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    try:
                        with zip_ref.open('backup_summary.txt') as f:
                            summary = f.read().decode('utf-8')
                    except:
                        summary = "无备份摘要信息"
                backup_files.append({
                    'file_name': file,
                    'backup_time': backup_time,
                    'file_path': file_path,
                    'summary': summary
                })
    except Exception as e:
        logger.error(f"获取备份列表失败: {str(e)}")
    return sorted(backup_files, key=lambda x: x['backup_time'], reverse=True)

def verify_backup(backup_path):
    """验证备份文件的完整性"""
    try:
        with zipfile.ZipFile(backup_path, 'r') as zip_ref:
            # 检查必要文件是否存在
            files = zip_ref.namelist()
            if 'backup_summary.txt' not in files:
                return False, "备份文件缺少摘要信息"
            
            # 读取摘要信息
            with zip_ref.open('backup_summary.txt') as f:
                summary = f.read().decode('utf-8')
            
            # 验证所有JSON文件是否可读
            json_files = [f for f in files if f.endswith('.json')]
            for json_file in json_files:
                with zip_ref.open(json_file) as f:
                    try:
                        json.load(f)
                    except json.JSONDecodeError:
                        return False, f"文件 {json_file} 包含无效的JSON数据"
            
            return True, summary
    except Exception as e:
        return False, f"备份文件验证失败: {str(e)}"

def create_backup_dir(schema_name):
    """创建备份目录"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(BACKUP_PATH, f'{schema_name}_{timestamp}')
    os.makedirs(backup_dir, exist_ok=True)
    return backup_dir

def serialize_spatial_info(db_data):
    """序列化空间信息"""
    if 'F_SPATIAL_INFO' in db_data and db_data['F_SPATIAL_INFO'] is not None:
        try:
            # 存储为标准格式
            db_data['F_SPATIAL_INFO'] = {
                'type': 'SDO_GEOMETRY',
                'srid': 4326,
                'gtype': 2003,
                'coordinates': [
                    (db_data['F_TOPLEFTLONGITUDE'], db_data['F_TOPLEFTLATITUDE']),
                    (db_data['F_TOPRIGHTLONGITUDE'], db_data['F_TOPRIGHTLATITUDE']),
                    (db_data['F_BOTTOMRIGHTLONGITUDE'], db_data['F_BOTTOMRIGHTLATITUDE']),
                    (db_data['F_BOTTOMLEFTLONGITUDE'], db_data['F_BOTTOMLEFTLATITUDE']),
                    (db_data['F_TOPLEFTLONGITUDE'], db_data['F_TOPLEFTLATITUDE'])  # 闭合多边形
                ]
            }
        except Exception as e:
            logger.error(f"空间信息序列化失败: {str(e)}")
            db_data['F_SPATIAL_INFO'] = None
    return db_data

def backup_table(pool, schema_name, table_name, backup_dir):
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                # 先获取表的所有列名
                cursor.execute(f"SELECT * FROM {schema_name}.{table_name} WHERE ROWNUM < 1")
                all_columns = [col[0] for col in cursor.description]
                
                # 构建查询语句，特殊处理日期和空间字段
                select_parts = []
                for col in all_columns:
                    if col in ['F_PRODUCETIME', 'F_RECEIVETIME', 'F_IMPORTDATE']:
                        # Oracle DATE 类型的正确处理方式
                        select_parts.append(
                            f"""CASE 
                                WHEN {col} IS NOT NULL THEN 
                                    TO_CHAR({col}, 'YYYY-MM-DD HH24:MI:SS')
                                ELSE NULL 
                            END as {col}"""
                        )
                    elif col == 'F_SPATIAL_INFO':
                        select_parts.append(
                            f"""CASE 
                                WHEN {col} IS NOT NULL THEN 
                                    SDO_UTIL.TO_WKTGEOMETRY({col})
                                ELSE NULL 
                            END as {col}"""
                        )
                    else:
                        select_parts.append(col)
                
                # 构建并执行查询
                query = f"""
                    SELECT {', '.join(select_parts)}
                    FROM {schema_name}.{table_name}
                """
                
                # 先测试查询是否正确
                try:
                    cursor.execute(query)
                except Exception as e:
                    logger.error(f"查询执行失败: {str(e)}\nSQL: {query}")
                    raise
                
                rows = cursor.fetchall()
                
                # 处理结果
                data = []
                for row in rows:
                    db_data = dict(zip(all_columns, row))
                    
                    # 为日期字段添加毫秒部分
                    for date_field in ['F_PRODUCETIME', 'F_RECEIVETIME', 'F_IMPORTDATE']:
                        if date_field in db_data and db_data[date_field]:
                            # 确保日期格式统一
                            if isinstance(db_data[date_field], str):
                                # 如果是字符串，添加毫秒部分
                                db_data[date_field] = f"{db_data[date_field]}.000"
                            elif isinstance(db_data[date_field], datetime):
                                # 如果是datetime对象，格式化为字符串
                                db_data[date_field] = db_data[date_field].strftime('%Y-%m-%d %H:%M:%S.000')
                    
                    # 处理空间信息
                    if db_data.get('F_SPATIAL_INFO'):
                        db_data = serialize_spatial_info(db_data)
                    data.append(db_data)
                
                # 保存到文件
                output_file = os.path.join(backup_dir, f"{table_name}.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2, default=str)
                
                return len(data)
                
    except Exception as e:
        logger.error(f"备份表 {table_name} 时发生错误: {str(e)}")
        raise Exception(f"备份表 {table_name} 失败: {str(e)}")

def create_backup_summary(backup_dir, schema_name, successful_tables, failed_tables):
    """创建备份摘要文件"""
    summary_file = os.path.join(backup_dir, 'backup_summary.txt')
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"备份时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"模式名称: {schema_name}\n\n")
        
        if successful_tables:
            f.write("成功备份的表:\n")
            for table, count in successful_tables:
                f.write(f"  - {table}: {count} 条记录\n")
        
        if failed_tables:
            f.write("\n备份失败的表:\n")
            for table, error in failed_tables:
                f.write(f"  - {table}: {error}\n")

def compress_backup(backup_dir):
    """压缩备份文件夹"""
    try:
        shutil.make_archive(backup_dir, 'zip', backup_dir)
        shutil.rmtree(backup_dir)
        return f"{backup_dir}.zip"
    except Exception as e:
        raise Exception(f"压缩备份文件失败: {str(e)}")


def create_sdo_geometry(conn, coordinates):
    """创建SDO_GEOMETRY对象"""
    try:
        sdo_geometry = conn.gettype("MDSYS.SDO_GEOMETRY")
        sdo_elem_info = conn.gettype("MDSYS.SDO_ELEM_INFO_ARRAY")
        sdo_ordinate = conn.gettype("MDSYS.SDO_ORDINATE_ARRAY")
        
        obj = sdo_geometry.newobject()
        obj.SDO_GTYPE = 2003
        obj.SDO_SRID = 4326
        obj.SDO_ELEM_INFO = sdo_elem_info.newobject()
        obj.SDO_ELEM_INFO.extend([1, 1003, 1])
        
        obj.SDO_ORDINATES = sdo_ordinate.newobject()
        for coord in coordinates:
            obj.SDO_ORDINATES.extend([coord[0], coord[1]])
            
        return obj
    except Exception as e:
        logger.error(f"创建SDO_GEOMETRY对象失败: {str(e)}")
        return None