import threading
import json
from datetime import datetime
import base64
from oracledb import DbObject, BLOB
from src.utils.logger import logger

class Mapper:
    def __init__(self, pool):
        self.pool = pool
        self.lock = threading.Lock()

    def executeQuery(self, sql, params=None):
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    result = cursor.fetchall()
                    return result
        except Exception as e:
            logger.error(f"SQL查询错误: {str(e)}, SQL: {sql}, params: {params}")
            return None

    def executeNonQuery(self, sql, params=None):
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    conn.commit()
                    return True
        except Exception as e:
            logger.error(f"SQL执行错误: {str(e)}, SQL: {sql}, params: {params}")
            return False

    def insert_data_into_table(self, table_name, data):
        """插入数据到指定表"""
        try:
            # 准备插入数据
            insert_data = self._prepare_insert_data(data)
            if not insert_data:
                return False

            # 构建SQL语句
            fields = []
            values = []
            
            # 处理常规字段
            for key in insert_data.keys():
                fields.append(key)
                if key in ['F_PRODUCETIME', 'F_RECEIVETIME', 'F_IMPORTDATE']:
                    # 使用 TIMESTAMP 来处理带毫秒的日期
                    values.append("TO_TIMESTAMP(:" + key + ", 'YYYY-MM-DD HH24:MI:SS.FF3')")
                else:
                    values.append(f":{key}")

            # 构建完整SQL
            sql = f"""
                INSERT INTO {table_name} (
                    {', '.join(fields)}
                ) VALUES (
                    {', '.join(values)}
                )
            """

            # 执行插入
            success = self.executeNonQuery(sql, insert_data)
            if not success:
                logger.error(f"插入失败的数据: {insert_data}")
            return success

        except Exception as e:
            logger.error(f"插入数据错误: {str(e)}")
            return False

    def _prepare_insert_data(self, data):
        """准备插入数据，处理日期和特殊字段"""
        try:
            insert_data = data.copy()
            
            # 处理日期字段
            for field in ['F_PRODUCETIME', 'F_RECEIVETIME', 'F_IMPORTDATE']:
                if field in insert_data and insert_data[field]:
                    try:
                        if isinstance(insert_data[field], str):
                            # 移除可能存在的时区信息
                            date_str = insert_data[field].split('+')[0].strip()
                            
                            # 确保日期字符串有毫秒部分
                            if '.' not in date_str:
                                date_str += '.000'
                            else:
                                # 确保毫秒部分是3位
                                main_part, ms_part = date_str.split('.')
                                ms_part = ms_part[:3].ljust(3, '0')
                                date_str = f"{main_part}.{ms_part}"
                            
                            try:
                                # 尝试解析完整格式（带毫秒）
                                dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
                            except ValueError:
                                try:
                                    # 尝试解析不带毫秒的格式
                                    dt = datetime.strptime(date_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
                                except ValueError:
                                    # 尝试解析ISO格式
                                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            
                            # 统一转换为带3位毫秒的格式
                            insert_data[field] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        elif isinstance(insert_data[field], datetime):
                            # 确保datetime对象也输出3位毫秒
                            insert_data[field] = insert_data[field].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    except Exception as e:
                        logger.error(f"日期格式转换失败 {field}: {record[field]} - {str(e)}")
                        insert_data[field] = None

            # 过滤空值
            insert_data = {k: v for k, v in insert_data.items() if v is not None}
            return insert_data

        except Exception as e:
            logger.error(f"准备插入数据失败: {str(e)}")
            return None

    def create_sdo_geometry(self, coords):
        """创建Oracle SDO_GEOMETRY对象"""
        try:
            with self.pool.acquire() as conn:
                coords_str = ','.join(f'{lon},{lat}' for lon, lat in coords)
                sql = f"""
                SELECT SDO_GEOMETRY(
                    2003,  -- 2D多边形
                    4326,  -- WGS84 SRID
                    NULL,
                    SDO_ELEM_INFO_ARRAY(1, 1003, 1),  -- 外部多边形
                    SDO_ORDINATE_ARRAY({coords_str})
                ) FROM DUAL
                """
                
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    return result[0] if result else None
                    
        except Exception as e:
            logger.error(f"创建SDO_GEOMETRY对象失败: {str(e)}")
            return None

def serialize(obj):
    """序列化对象为JSON格式"""
    try:
        if obj is None:
            return None
            
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
        if isinstance(obj, (BLOB, DbObject)):
            try:
                return base64.b64encode(obj.read()).decode('utf-8')
            except:
                return str(obj)
                
        if isinstance(obj, dict):
            return {k: serialize(v) for k, v in obj.items()}
            
        if isinstance(obj, list):
            return [serialize(item) for item in obj]
            
        return obj
        
    except Exception as e:
        logger.error(f"序列化失败: {str(e)}, 对象类型: {type(obj)}")
        return str(obj)

def deserialize(obj):
    """反序列化JSON数据为Python对象"""
    try:
        if isinstance(obj, str):
            # 处理日期时间字符串
            try:
                if 'T' in obj:
                    return datetime.fromisoformat(obj.replace('Z', '+00:00'))
                if ':' in obj:
                    try:
                        return datetime.strptime(obj, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        return datetime.strptime(obj, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                pass
                
        if isinstance(obj, dict):
            return {k: deserialize(v) for k, v in obj.items()}
            
        if isinstance(obj, list):
            return [deserialize(item) for item in obj]
            
        return obj
        
    except Exception as e:
        logger.error(f"反序列化失败: {str(e)}, 对象类型: {type(obj)}")
        return obj