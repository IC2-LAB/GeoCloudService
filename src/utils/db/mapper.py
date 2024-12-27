import threading
import json
import base64
import oracledb
from src.utils.logger import logger

class Mapper:
    def __init__(self, pool):
        self.pool = pool
        self.lock = threading.Lock()
        
    def executeQuery(self, sql, params=None):
        """执行查询语句"""
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    columns = [col[0] for col in cursor.description]
                    result = cursor.fetchall()
                    return [dict(zip(columns, row)) for row in result]
        except Exception as e:
            logger.error(f"SQL查询错误: {str(e)}, SQL: {sql}, params: {params}")
            return []
    
    def executeNonQuery(self, sql, params=None):
        """执行非查询语句"""
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    conn.commit()
                    return True
        except Exception as e:
            logger.error(f"SQL执行: {str(e)}, SQL: {sql}, params: {params}")
            return False
            
    def getDataByReceiveTime(self, start_time, end_time, table_name):
        """根据接收时间或开始时间获取数据，并处理空间信息"""
        try:
            # 定义使用 F_STARTTIME 的表名列表
            use_starttime_tables = [
                "GF5_VIMSDATA","GF5_AHSIDATA","ZY1F_AHSI","ZY1F_ISR_NSR",
            ]
            
            # 根据表名选择不同的时间字段
            time_field = "F_STARTTIME" if table_name in use_starttime_tables else "F_RECEIVETIME"
            
            # 首先检查表是否有 F_SPATIAL_INFO 字段
            check_column_query = """
                SELECT COLUMN_NAME 
                FROM ALL_TAB_COLUMNS 
                WHERE TABLE_NAME = :table_name
                AND OWNER = 'PDBADMIN'
                AND COLUMN_NAME = 'F_SPATIAL_INFO'
            """
            
            has_spatial_info = False
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(check_column_query, {'table_name': table_name})
                    if cursor.fetchone():
                        has_spatial_info = True
            
            # 根据是否有空间信息字段�����询
            if has_spatial_info:
                query = f"""
                    SELECT 
                        a.*,
                        SDO_UTIL.TO_WKTGEOMETRY(a.F_SPATIAL_INFO) as GEOMETRY
                    FROM PDBADMIN.{table_name} a
                    WHERE {time_field} BETWEEN 
                        TO_TIMESTAMP(:start_time, 'YYYY-MM-DD HH24:MI:SS.FF3') 
                        AND TO_TIMESTAMP(:end_time, 'YYYY-MM-DD HH24:MI:SS.FF3')
                """
            else:
                query = f"""
                    SELECT 
                        a.*
                    FROM PDBADMIN.{table_name} a
                    WHERE {time_field} BETWEEN 
                        TO_TIMESTAMP(:start_time, 'YYYY-MM-DD HH24:MI:SS.FF3') 
                        AND TO_TIMESTAMP(:end_time, 'YYYY-MM-DD HH24:MI:SS.FF3')
                """
            
            results = []
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, {
                        'start_time': start_time,
                        'end_time': end_time
                    })
                    columns = [col[0] for col in cursor.description]
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        data = dict(zip(columns, row))
                        
                        # 只有在有空间信息时才处理 GEOMETRY
                        if has_spatial_info:
                            if 'GEOMETRY' in data and data['GEOMETRY']:
                                data['F_SPATIAL_INFO'] = data['GEOMETRY']
                            else:
                                data['F_SPATIAL_INFO'] = None
                                
                            if 'GEOMETRY' in data:
                                del data['GEOMETRY']
                           
                        results.append(data)
                        
            return results
        except Exception as e:
            logger.error(f"获取数据错误: {str(e)}")
            return []

    def getGraphData(self, table_name):
        """获取表中的所有数据，包括BLOB字段"""
        try:
            # 首先获取表的列信息（从 JGF_GXFW 模式）
            columns_query = f"""
                SELECT COLUMN_NAME 
                FROM ALL_TAB_COLUMNS 
                WHERE TABLE_NAME = :table_name
                AND OWNER = 'JGF_GXFW'
            """
            
            results = []
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    # 获取表的信息
                    cursor.execute(columns_query, {'table_name': table_name})
                    columns = [row[0] for row in cursor.fetchall()]
                    
                    # 构建查询语句
                    select_parts = []
                    for col in columns:
                        select_parts.append(f'a.{col}')
                    
                    if 'F_SPATIAL_INFO' in columns:
                        select_parts.append('SDO_UTIL.TO_WKTGEOMETRY(a.F_SPATIAL_INFO) as GEOMETRY')
                    
                    # 获取总记录数
                    count_query = f"SELECT COUNT(*) FROM {table_name}"
                    cursor.execute(count_query)
                    total_count = cursor.fetchone()[0]
                    logger.info(f"表 {table_name} 总记录数: {total_count}")
                    
                    # 分批次查询数据
                    page_size = 1000
                    for offset in range(0, total_count, page_size):
                        paged_query = f"""
                            SELECT * FROM (
                                SELECT a.*, ROWNUM as rnum 
                                FROM (
                                    SELECT {', '.join(select_parts)}
                                    FROM JGF_GXFW.{table_name} a
                                    ORDER BY a.F_DID
                                ) a 
                                WHERE ROWNUM <= :end_row
                            ) 
                            WHERE rnum > :start_row
                        """
                        
                        cursor.execute(paged_query, {
                            'start_row': offset,
                            'end_row': offset + page_size
                        })
                        
                        columns = [col[0] for col in cursor.description]
                        rows = cursor.fetchall()
                        
                        for row in rows:
                            data = {}
                            for i, value in enumerate(row):
                                column_name = columns[i]
                                if column_name == 'RNUM':
                                    continue
                                    
                                if value is not None and hasattr(value, 'read'):
                                    try:
                                        blob_data = value.read()
                                        if blob_data:
                                            data[column_name] = base64.b64encode(blob_data).decode('utf-8')
                                        else:
                                            data[column_name] = None
                                    except Exception as e:
                                        logger.error(f"处理BLOB数据错误: {str(e)}")
                                        data[column_name] = None
                                elif column_name == 'GEOMETRY' and value:
                                    data['F_SPATIAL_INFO'] = value
                                else:
                                    data[column_name] = value
                            
                            results.append(data)
                        
                        logger.info(f"已处理 {len(results)}/{total_count} 条记录")
                    
                    logger.info(f"从表 {table_name} 获取到 {len(results)} 条数据")
                    return results
                    
        except Exception as e:
            logger.error(f"获取图形数据错误: {str(e)}")
            return []

    def getGraphDataByDataID(self, data_id):
        """根据 F_DATAID 从图像数据表获取数据"""
        try:
            query = f"""
                SELECT 
                    a.*
                FROM JGF_GXFW.TB_BAS_META_BLOB a
                WHERE a.F_DID = :data_id
            """
            
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, {'data_id': data_id})
                    columns = [col[0] for col in cursor.description]
                    rows = cursor.fetchall()
                    
                    if not rows:
                        return None
                    
                    row = rows[0]  # 只处理第一条记录
                    data = {}
                    for i, value in enumerate(row):
                        column_name = columns[i]
                        
                        # 处理BLOB数据
                        if value is not None and hasattr(value, 'read'):
                            try:
                                blob_data = value.read()
                                if blob_data:
                                    data[column_name] = base64.b64encode(blob_data).decode('utf-8')
                                else:
                                    data[column_name] = None
                            except Exception as e:
                                logger.error(f"处理BLOB数据错误: {str(e)}")
                                data[column_name] = None
                        else:
                            data[column_name] = value
                    
                    return data
                
        except Exception as e:
            logger.error(f"根据DataID={data_id}获取图形数据错误: {str(e)}")
            return None

    def insertGraphData(self, data):
        """将JSON数据（包含base64编码的BLOB）插入到TB_BAS_META_BLOB表"""
        try:
            # 准备SQL语句，动态生成列名和占位符
            columns = []
            values = []
            bind_params = {}
            
            # BLOB字段列表
            blob_fields = ['F_THUMIMAGE', 'F_QUICKIMAGE', 'F_SHAPEIMAGE', 'F_GRAPH']
            
            with self.pool.acquire() as conn:
                cursor = conn.cursor()
                
                for key, value in data.items():
                    if value is not None:  # 只处理非空值
                        columns.append(key)
                        values.append(f":{key}")
                        
                        # 如果是BLOB字段且有数据
                        if key in blob_fields and value:
                            try:
                                # 直接使用 bytes 对象，oracledb 会自动处理
                                blob_data = base64.b64decode(value)
                                bind_params[key] = blob_data
                            except Exception as e:
                                logger.error(f"BLOB据解码错误 ({key}): {str(e)}")
                                bind_params[key] = None
                        else:
                            bind_params[key] = value

                # 构建INSERT语句
                sql = f"""
                    INSERT INTO TB_BAS_META_BLOB (
                        {', '.join(columns)}
                    ) VALUES (
                        {', '.join(values)}
                    )
                """
                
                try:
                    cursor.execute(sql, bind_params)
                    conn.commit()
                    logger.info(f"成功插入BLOB数据: F_DID = {data.get('F_DID', 'unknown')}")
                    return True
                except Exception as e:
                    logger.error(f"插入数据错误: {str(e)}")
                    logger.error(f"SQL: {sql}")
                    logger.error(f"列: {columns}")
                    return False
                finally:
                    cursor.close()
                    
        except Exception as e:
            logger.error(f"处理数据插入错误: {str(e)}")
            return False

    def insert_satellite_data(self, table_name, data):
        """将卫��数据插入到指定表"""
        try:
            # 字段映射关系
            field_mapping = {
                # 直接映射的字段
                'F_DATANAME': 'F_DATANAME',
                'F_PRODUCTID': 'F_PRODUCTID',
                'F_PRODUCTLEVEL': 'F_PRODUCTLEVEL',
                'F_SATELLITEID': 'F_SATELLITEID',
                'F_SENSORID': 'F_SENSORID',
                'F_CLOUDPERCENT': 'F_CLOUDPERCENT',
                'F_ORBITID': 'F_ORBITID',
                'F_SCENEID': 'F_SCENEID',
                'F_SCENEPATH': 'F_SCENEPATH',
                'F_SCENEROW': 'F_SCENEROW',
                'F_IMPORTUSER': 'F_IMPORTUSER',
                'F_DATASIZE': 'F_DATASIZE',
                'F_PITCHSATELLITEANGLE': 'F_PITCHSATELLITEANGLE',
                'F_PITCHVIEWINGANGLE': 'F_PITCHVIEWINGANGLE',
                'F_YAWSATELLITEANGLE': 'F_YAWSATELLITEANGLE',
                'F_ROLLSATELLITEANGLE': 'F_ROLLSATELLITEANGLE',
                'F_ROLLVIEWINGANGLE': 'F_ROLLVIEWINGANGLE',
                'F_TABLENAME': 'F_TABLENAME',
                
                # 经纬度字段映射
                'F_UPPERLEFTLAT': 'F_TOPLEFTLATITUDE',
                'F_UPPERLEFTLONG': 'F_TOPLEFTLONGITUDE',
                'F_UPPERRIGHTLAT': 'F_TOPRIGHTLATITUDE',
                'F_UPPERRIGHTLONG': 'F_TOPRIGHTLONGITUDE',
                'F_BOTTOMRIGHTLAT': 'F_BOTTOMRIGHTLATITUDE',
                'F_BOTTOMRIGHTLONG': 'F_BOTTOMRIGHTLONGITUDE',
                'F_BOTTOMLEFTLAT': 'F_BOTTOMLEFTLATITUDE',
                'F_BOTTOMLEFTLONG': 'F_BOTTOMLEFTLONGITUDE',
                
                # 其他特殊映射
                'F_IMAGEGSD': 'F_LOCATION',
                'F_IMPORTTIME': 'F_IMPORTDATE',  # 添加 F_IMPORTTIME 映射
            }

            # 准备插入数据
            insert_data = {}
            for src_field, dest_field in field_mapping.items():
                if src_field in data:
                    insert_data[dest_field] = data[src_field]

            # 特殊处理：将 F_DATAID 映射为 F_DID
            if 'F_DATAID' in data and data['F_DATAID'] is not None:
                insert_data['F_DID'] = data['F_DATAID']
            elif 'F_DID' in data and data['F_DID'] is not None:
                insert_data['F_DID'] = data['F_DID']
            else:
                # 如果没有 ID，生成一个新的
                with self.pool.acquire() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT MAX(F_DID) + 1 FROM JGF_GXFW.TB_META_GF1")
                        new_id = cursor.fetchone()[0]
                        insert_data['F_DID'] = new_id if new_id else 1

            # 处理时间字段 - 从ISO格式转换为Oracle日期格式
            time_fields = {
                'F_PRODUCETIME': data.get('F_PRODUCETIME', '').replace('T', ' ').replace('Z', ''),
                'F_RECEIVETIME': data.get('F_RECEIVETIME', '').replace('T', ' ').replace('Z', ''),
                'F_IMPORTDATE': data.get('F_IMPORTTIME', '').replace('T', ' ').replace('Z', '')  # 使用 F_IMPORTTIME
            }

            # 生成空间信息
            if all(key in insert_data for key in ['F_TOPLEFTLONGITUDE', 'F_TOPLEFTLATITUDE', 
                                                'F_TOPRIGHTLONGITUDE', 'F_TOPRIGHTLATITUDE',
                                                'F_BOTTOMRIGHTLONGITUDE', 'F_BOTTOMRIGHTLATITUDE',
                                                'F_BOTTOMLEFTLONGITUDE', 'F_BOTTOMLEFTLATITUDE']):
                # 构建多边形的坐标数组
                coordinates = [
                    insert_data['F_TOPLEFTLONGITUDE'], insert_data['F_TOPLEFTLATITUDE'],
                    insert_data['F_TOPRIGHTLONGITUDE'], insert_data['F_TOPRIGHTLATITUDE'],
                    insert_data['F_BOTTOMRIGHTLONGITUDE'], insert_data['F_BOTTOMRIGHTLATITUDE'],
                    insert_data['F_BOTTOMLEFTLONGITUDE'], insert_data['F_BOTTOMLEFTLATITUDE'],
                    insert_data['F_TOPLEFTLONGITUDE'], insert_data['F_TOPLEFTLATITUDE']  # 闭合多边形
                ]
                
                if all(coord is not None for coord in coordinates):
                    # 构建 SDO_GEOMETRY
                    spatial_sql = f"""
                        SDO_GEOMETRY(
                            2003,  -- 2D polygon
                            4326,  -- SRID: WGS84
                            NULL,
                            SDO_ELEM_INFO_ARRAY(1,1003,1),  -- exterior polygon
                            SDO_ORDINATE_ARRAY({','.join(map(str, coordinates))})
                        )
                    """
                    insert_data['F_SPATIAL_INFO'] = spatial_sql

            # 构建 SQL 语句
            columns = list(insert_data.keys())
            # 添加不在 insert_data 中的时间字段
            for time_field in time_fields:
                if time_field not in columns:
                    columns.append(time_field)

            values = []
            bind_params = {}

            for col in columns:
                if col in time_fields:
                    if time_fields[col]:
                        values.append(f"TO_TIMESTAMP(:{col}, 'YYYY-MM-DD HH24:MI:SS.FF3')")
                        bind_params[col] = time_fields[col]
                    else:
                        values.append("NULL")
                elif col == 'F_SPATIAL_INFO' and 'F_SPATIAL_INFO' in insert_data:
                    values.append(insert_data['F_SPATIAL_INFO'])
                else:
                    values.append(f":{col}")
                    bind_params[col] = insert_data[col]

            sql = f"""
                INSERT INTO JGF_GXFW.{table_name} (
                    {', '.join(columns)}
                ) VALUES (
                    {', '.join(values)}
                )
            """

            with self.pool.acquire() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, bind_params)
                    conn.commit()
                    logger.info(f"成功插入数据到 {table_name}: F_DATANAME = {data.get('F_DATANAME', 'unknown')}")
                    return True
                except Exception as e:
                    logger.error(f"插入数据错误: {str(e)}")
                    logger.error(f"SQL: {sql}")
                    logger.error(f"数据: {bind_params}")
                    return False
                finally:
                    cursor.close()

        except Exception as e:
            logger.error(f"处理数据插入错误: {str(e)}")
            return False

