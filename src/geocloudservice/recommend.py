from datetime import datetime
from src.utils.GeoProcessor import GeoProcessor
from src.utils.GeoDBHandler import GeoDBHandler
from src.utils.logger import logger
from concurrent.futures import ThreadPoolExecutor
from src.utils.Email import send_email
from src.utils.IdMaker import getPkId
from src.utils.db.oracle import executeNonQuery, executeQuery
from src.config.config import satelliteToNodeId, NodeIdToNodeName
from src.utils.CacheManager import CacheManager

def fetchDataFromDB(pool, sql:str ,param=None):
    """从数据库中获取数据和字段名"""
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, param)
                # 默认最后一个字段名是几何字段，舍弃
                columns = [desc[0] for desc in cur.description[:-1]]
                data = cur.fetchall()
        return data, columns
    except Exception as e:
        logger.error(f'从数据库中获取数据失败: {e}, sql: {sql}, param: {param}')
        return None, None

def getTargetArea(geodbhandler: GeoDBHandler, wkt: str, areaCode: str, pool):
    """根据传入的wkt和行政区划代码获取目标区域的几何形状""" 
    try:  
        if wkt is not None:
            target_area = geodbhandler.wktToShapely(wkt)
        elif areaCode is not None:
            target_area = getShapelyAreaByCode(areaCode, pool)
        return target_area
    except Exception as e:
        logger.error(f'获取目标区域失败: {e}')
        return None

def cacheFetchRecommendData(tablename: list, wkt: str, areacode: str , pool, 
                            cache: CacheManager, guid: str, page: int, pagesize: int = 30 ) ->list:
    cacheKey = cache.getCacheKey('fetchRecommendData', guid)
    cacheData = cache.getData(cacheKey)
    
    if cacheData is not None:
        geoData, coverageRatio = cacheData
    else:
        geoData, coverageRatio = fetchRecommendData(tablename, wkt, areacode, pool)
        cache.setData(cacheKey, (geoData, coverageRatio))
      
    geoprocessor = GeoProcessor()
    geoDataDict = geoprocessor.GeoDataFrameToDict(geoData)
    formattedDataDict = formatDictForView(geoDataDict)  
    
    startIndex = (page - 1) * pagesize
    endIndex = page * pagesize
    paginatedData = formattedDataDict[startIndex:endIndex]
    return paginatedData, coverageRatio
    
    
def fetchRecommendData(tablename: list, wkt: str, areacode: str , pool):
    """一键推荐功能具体实现

    Args:
        tablename (list): 需要查询的表名列表(与卫星绑定)
        wkt (str): 检索区域的wkt
        areacode (str): 检索区域的行政区划代码
        pool (_type_): 数据库连接池
        areacode和wkt能且只能有一个不为空

    Returns:
        与目标区域相交的数据,GeoDataFrame格式
        覆盖率
    """
    
    if wkt is None and areacode is None:
        logger.error('wkt和areacode不能同时为空')
        return None
    dataname = ["F_DATANAME", "F_DID", "F_SCENEROW", "F_LOCATION", "F_PRODUCTID", "F_PRODUCTLEVEL",
                "F_CLOUDPERCENT", "F_TABLENAME", "F_DATATYPENAME", "F_ORBITID", "F_PRODUCETIME",
                "F_SENSORID", "F_DATASIZE", "F_RECEIVETIME", "F_DATAID", "F_SATELLITEID", "F_SCENEPATH",
                "F_SPATIAL_INFO"]
    whereSql = "WHERE F_TOPLEFTLATITUDE <= :maxlat AND F_TOPLEFTLONGITUDE >= :minlon \
        AND F_BOTTOMRIGHTLATITUDE >= :minlat AND F_BOTTOMRIGHTLONGITUDE <= :maxlon \
            AND F_CLOUDPERCENT <= 20"
    selectSql = generateSqlQuery(dataname, tablename, whereSql)
    ordersql = ' ORDER BY "F_RECEIVETIME" DESC FETCH FIRST :limit_num ROWS ONLY'
    sql = f'{selectSql} {ordersql}'
    geodbhandler = GeoDBHandler()
    geoprocessor = GeoProcessor()
    target_area = getTargetArea(geodbhandler, wkt, areacode, pool)
    (minlon, maxlon, minlat, maxlat) = geoprocessor.getCoordinateRange(target_area)
    coverage_ratio = 0
    n = 1
    try:
        while coverage_ratio < 0.9 and n < 9:
            limit_num = 100 * n
            data, columns = fetchDataFromDB(pool, sql, {'limit_num': limit_num, 'minlon': minlon, 'maxlon': maxlon, 'minlat': minlat, 'maxlat': maxlat})
            data_gdf = geodbhandler.imageDataToGeoDataFrame(data, columns)
            intersected_data = geoprocessor.findIntersectedData(target_area, data_gdf)
            coverage_ratio = geoprocessor.calCoverageRatio(target_area, intersected_data)
            n += 1
        return intersected_data, coverage_ratio
    except Exception as e:
        logger.error(f'推荐数据失败: {e}')
        return None

def cacheFeachRecomCoverData(tablename: list, wkt: str, areacode: str , cache: CacheManager, guid: str, pool) ->dict:
    cacheKey = cache.getCacheKey('fetchRecommendData', guid)
    cacheData = cache.getData(cacheKey)
    if cacheData is not None:
        geoData, _ = cacheData
    else:
        geoData, coverageRatio = fetchRecommendData(tablename, wkt, areacode, pool)
        cache.setData(cacheKey, (geoData, coverageRatio))
    
    sizenum = len(geoData)
    geoprocessor = GeoProcessor()
    combine_wkt, total_area = geoprocessor.calculateMergedArea(geoData)
    return sizenum, combine_wkt, total_area, 1

def cacheFeachSearchData(tablename: list, wkt: str, areacode: str, startTime: str, endTime: str, cloudPercent: str, cache: CacheManager, guid: str, pool) ->list:
    cacheKey = cache.getCacheKey('searchData', guid)
    cacheData = cache.getData(cacheKey)
    if cacheData is not None:
        return cacheData
    else:
        geoData = searchData(tablename, wkt, areacode, startTime, endTime, cloudPercent, pool)
        cache.setData(cacheKey, geoData)
        return geoData
    
def searchData(tablename: list, wkt :str, areacode : str, startTime: str, endTime: str, cloudPercent: str, pool) ->list:
    """检索功能具体实现

    Args:
        tablename (list): 需要查询的数据表名
        wkt (str): 检索区域的wkt
        areacode (str): 检索区域的行政区划代码
        startTime (str): 影像数据开始时间
        endTime (str): 影像数据结束时间
        cloudPercent (str): 云量

    Returns:
        list: 字典列表, 每一条字典代表一条数据
    """
    try:
        if wkt is None and areacode is None:
            logger.error('wkt和areacode不能同时为空')
            return None
        dataname = ["F_DATANAME", "F_DID", "F_SCENEROW", "F_LOCATION", "F_PRODUCTID", "F_PRODUCTLEVEL",
                    "F_CLOUDPERCENT", "F_TABLENAME", "F_DATATYPENAME", "F_ORBITID", "F_PRODUCETIME",
                    "F_SENSORID", "F_DATASIZE", "F_RECEIVETIME", "F_DATAID", "F_SATELLITEID", "F_SCENEPATH",
                    "F_SPATIAL_INFO"]
        whereSql = " WHERE  F_RECEIVETIME BETWEEN TO_DATE(:startTime, \'YYYY-MM-DD HH24:MI:SS\') AND TO_DATE(:endTime, \'YYYY-MM-DD HH24:MI:SS\') AND F_CLOUDPERCENT <= :cloudPercent"
        selectSql = generateSqlQuery(dataname, tablename, whereSql)
        orderSql = ' ORDER BY "F_RECEIVETIME" DESC '
        sql = f'{selectSql} {orderSql}'
        geodbhandler = GeoDBHandler()
        target_area = getTargetArea(geodbhandler, wkt, areacode, pool)
        geoprocessor = GeoProcessor()
        ImageInfo, columns = fetchDataFromDB(pool, sql, {'startTime': startTime, 'endTime': endTime, 'cloudPercent': cloudPercent})
        ImageGdf = geodbhandler.imageDataToGeoDataFrame(ImageInfo, columns)
        intersected_data = geoprocessor.findIntersectedData(target_area, ImageGdf)
        result = geoprocessor.GeoDataFrameToDict(intersected_data)
        formatted_result = formatDictForView(result)
        return formatted_result
    except Exception as e:
        logger.error(f'检索数据失败: {e}')
        return None
    
def querySubscribedData(tablename: list, wkt: str, areacode: str, startTime: str, endTime: str, cloudPercent: str, pool) -> list:
    """订阅功能具体实现

    Args:
        tablename (list): 需要查询的数据表名
        wkt (str): 检索区域的wkt
        areacode (str): 检索区域的行政区划代码
        startTime (str): 影像数据开始时间
        endTime (str): 影像数据结束时间
        cloudPercent (str): 云量
        userid (str): 用户id
        pool (_type_): 数据库连接池
    """
    
    try:
        if wkt is None and areacode is None:
            logger.error('wkt和areacode不能同时为空')
            return None
        dataname = ["F_DATANAME", "F_SPATIAL_INFO"]
        whereSql = ' WHERE  F_RECEIVETIME BETWEEN TO_DATE(:startTime, \'YYYY-MM-DD HH24:MI:SS\') AND TO_DATE(:endTime, \'YYYY-MM-DD HH24:MI:SS\') AND F_CLOUDPERCENT <= :cloudPercent'
        selectSql = generateSqlQuery(dataname, tablename, whereSql)
        orderSql = ' ORDER BY "F_RECEIVETIME" DESC '
        sql = f'{selectSql} {orderSql}'
        ImageInfo, columns = fetchDataFromDB(pool, sql, {'startTime': startTime, 'endTime': endTime, 'cloudPercent': cloudPercent})
        geodbhandler = GeoDBHandler()
        ImageGdf = geodbhandler.imageDataToGeoDataFrame(ImageInfo, columns)
        target_area = getTargetArea(geodbhandler, wkt, areacode, pool)
        geoprocessor = GeoProcessor()
        intersected_data = geoprocessor.findIntersectedData(target_area, ImageGdf)
        dataDict = geoprocessor.GeoDataFrameToDict(intersected_data)
        nameList = [data['F_DATANAME'] for data in dataDict]
        return nameList
    except Exception as e:
        logger.error(f'订阅数据失败: {e}')
        return None

def sendEmailToUser(userid: str, startTime: str, endTime: str, pool):
    """向用户发送邮件

    Args:
        userid (str): 用户id
        data (list): 查询数据名
        pool (_type_): 数据库连接池
    """
    def getUserEmail(userid: str):
        """获取用户的邮箱地址"""
        sql = "SELECT F_EMAIL FROM TC_SYS_USER WHERE F_ID = :F_ID"
        emailAddr, _ = fetchDataFromDB(pool, sql, {'F_ID': userid})
        return emailAddr[0][0]
    subject = "地质云卫星数据服务-数据订阅"
    message = f"您好，您订阅的 {startTime} - {endTime} 期间的数据已经上线【中国地质调查局自然资源航空物探遥感中心】"
    emailAddr = getUserEmail(userid)
    send_email(subject, message, emailAddr)
    logger.info(f'邮件发送成功: {message} to {emailAddr}')

def updateSubOrderStatus(pool, subid: str):
    """更新订阅订单状态"""
    try:
        logger.info(f'正在更新订阅{subid}状态')
        sql = 'UPDATE SUBSCRIBE_ORDER SET STATUS = 1 WHERE SUBID = :subid'
        executeNonQuery(pool, sql, {"subid": subid})
    except Exception as e:
        logger.error(f'更新订阅订单状态失败: {e}')
        return None
    
    
def ProcessDueSubscriptions(pool):
    """处理过期的订阅"""
    try:
        logger.info('正在处理过期订阅')
        dataname = ["USERID","SUBID","AREACODE","ISWKT","NODENAMES","CLOUDPERCENT",
                    "SUBTIME","SUBSTARTTIME","SUBENDTIME","WKT","STATUS"]
        tabelname = ["SUBSCRIBE_ORDER"]
        whereSql = ' WHERE  SUBENDTIME < SYSDATE AND STATUS = 0'
        selectSql = generateSqlQuery(dataname, tabelname, whereSql)
        data = executeQuery(pool, selectSql)
        
        try:
            for sub in data:
                IsWKT = sub[3]
                if(IsWKT == 1):
                    wkt = sub[9]
                    areacode = None
                else :
                    wkt = None
                    areacode = sub[2]
                userid = sub[0]
                subid = str(sub[1])
                tablenames = sub[4].split(',')
                startTime = str(sub[7])
                endTime = str(sub[8])
                cloudPercent = sub[5]
                logger.info(f'startTime: {startTime}, endTime: {endTime}, cloudPercent: {cloudPercent}')
                # datanames = querySubscribedData(tablenames, wkt, areacode, startTime, endTime, cloudPercent, userid, pool)
                dataInfos = searchData(tablenames, wkt, areacode, startTime, endTime, cloudPercent, pool)
                # sendEmailToUser(userid, datanames, pool)
                addDataToSubData(dataInfos, subid, pool)
                updateSubOrderStatus(pool, subid)
        except Exception as e:
            logger.error(f'用户{userid} 订阅订单 {subid} 处理失败: {e}')
            return None
        
    except Exception as e:
        logger.error(f'处理过期订阅失败: {e}')
        return None

def addDataToSubData(dataInfos: list, subid: str, pool):
    """将数据添加到订阅数据表"""
    sql = "INSERT INTO SUBSCRIBE_ORDERDATA ( \
            F_ID, F_ORDERID, F_DATANAME, F_SATELITE, F_SENSOR, \
                F_RECEIVETIME, F_DATASIZE, F_DATASOURCE, F_STATUS,\
                F_DATAPATH, F_DATATYPE, F_NODEID, F_DATAID, F_DOCNUM, \
                F_TM, F_PRODUCTLEVEL, \
                F_WKTRESPONSE, F_NODENAME, F_DOCNUM_OLD, F_CLOUDPERCENT, \
                F_SGTABLENAME, F_DID, F_ORBITID, F_SCENEPATH, \
                F_SCENEROW\
            ) VALUES ( \
                :F_ID, :F_ORDERID, :F_DATANAME, :F_SATELITE, :F_SENSOR, \
                TO_DATE(:F_RECEIVETIME, 'YYYY-MM-DD HH24:MI:SS'), :F_DATASIZE, \
                 :F_DATASOURCE, :F_STATUS,\
                :F_DATAPATH, :F_DATATYPE, :F_NODEID, :F_DATAID, :F_DOCNUM, \
                :F_TM, :F_PRODUCTLEVEL, \
                :F_WKTRESPONSE, :F_NODENAME, :F_DOCNUM_OLD, :F_CLOUDPERCENT, \
                :F_SGTABLENAME, :F_DID, :F_ORBITID, :F_SCENEPATH, \
                :F_SCENEROW) "
                
    def addSingleDataToSubData(dataInfo, subid):
        try:
            params = {}
            params['F_ID'] = getPkId()
            params['F_ORDERID'] = subid
            params['F_DATANAME'] = dataInfo['F_DATANAME']
            params['F_SATELITE'] = dataInfo['F_SATELLITEID']
            params['F_SENSOR'] = dataInfo['F_SENSORID']
            params['F_RECEIVETIME'] = dataInfo['F_RECEIVETIME']
            params['F_DATASIZE'] = float(dataInfo['F_DATASIZE'])
            params['F_DATASOURCE'] = None
            params['F_STATUS'] = None
            params['F_DATAPATH'] = None
            params['F_DATATYPE'] = 0
            params['F_NODEID'] = dataInfo['NODEID']
            params['F_DATAID'] = dataInfo['F_DATAID']
            params['F_DOCNUM'] = None
            params['F_TM'] = None
            params['F_PRODUCTLEVEL'] = dataInfo['F_PRODUCTLEVEL']
            params['F_WKTRESPONSE'] = dataInfo['WKTRESPONSE']
            params['F_NODENAME'] = NodeIdToNodeName[dataInfo['NODEID']]
            params['F_DOCNUM_OLD'] = None
            params['F_CLOUDPERCENT'] = float(dataInfo['F_CLOUDPERCENT'])
            params['F_SGTABLENAME'] = dataInfo['F_TABLENAME']
            params['F_DID'] = int(dataInfo['F_DID'])
            params['F_ORBITID'] = None if dataInfo['F_ORBITID'] == 'None' else int(dataInfo['F_ORBITID'])
            params['F_SCENEPATH'] = dataInfo['F_SCENEPATH']
            params['F_SCENEROW'] = dataInfo['F_SCENEROW']
            executeNonQuery(pool, sql, params)
        except Exception as e:
            logger.error('添加数据到订购数据失败: {}, 错误数据: {}, subid: {}'.format(e, dataInfo, subid))
            raise e
    
    executor = ThreadPoolExecutor()
    executor.map(addSingleDataToSubData, dataInfos, [subid]*len(dataInfos))     

def addDataToShop(dataInfos: list, userid, pool):
    """将数据添加到购物车"""
    sql = "INSERT INTO TF_SHOP ( \
            F_ID, F_USERID, F_DATANAME, F_SATELITE, F_SENSOR, \
            F_RECEIVETIME, F_DATASIZE, F_FAVORITETIME, F_DATASOURCE, \
            F_DATAPATH, F_DATATYPE, F_NODEID, F_DATAID, F_DOCNUM, \
            F_TM, F_DATATYPENAME, F_PRODUCTLEVEL, F_IMAGEURL, \
            F_WKTRESPONSE, F_NODENAME, F_DOCNUM_OLD, F_CLOUDPERCENT, \
            F_LOCATION, F_SGTABLENAME, F_DID, F_ORBITID, F_SCENEPATH, \
            F_SCENEROW, F_SYSTEMTYPE\
        ) VALUES ( \
            :F_ID, :F_USERID, :F_DATANAME, :F_SATELITE, :F_SENSOR, \
            TO_DATE(:F_RECEIVETIME, 'YYYY-MM-DD HH24:MI:SS'), :F_DATASIZE, \
            TO_DATE(:F_FAVORITETIME, 'YYYY-MM-DD HH24:MI:SS'), :F_DATASOURCE, \
            :F_DATAPATH, :F_DATATYPE, :F_NODEID, :F_DATAID, :F_DOCNUM, \
            :F_TM, :F_DATATYPENAME, :F_PRODUCTLEVEL, :F_IMAGEURL, \
            :F_WKTRESPONSE, :F_NODENAME, :F_DOCNUM_OLD, :F_CLOUDPERCENT, \
            :F_LOCATION, :F_SGTABLENAME, :F_DID, :F_ORBITID, :F_SCENEPATH, \
            :F_SCENEROW, :F_SYSTEMTYPE) "

    def addSingleDataToShop(dataInfo, userid):
        try:
            params = {}
            params['F_ID'] = getPkId()
            params['F_USERID'] = int(userid)
            params['F_DATANAME'] = dataInfo['F_DATANAME']
            params['F_SATELITE'] = dataInfo['F_SATELLITEID']
            params['F_SENSOR'] = dataInfo['F_SENSORID']
            params['F_RECEIVETIME'] = dataInfo['F_RECEIVETIME']
            params['F_DATASIZE'] = float(dataInfo['F_DATASIZE'])
            params['F_FAVORITETIME'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            params['F_DATASOURCE'] = None
            params['F_DATAPATH'] = None
            params['F_DATATYPE'] = 0
            params['F_NODEID'] = dataInfo['NODEID']
            params['F_DATAID'] = dataInfo['F_DATAID']
            params['F_DOCNUM'] = None
            params['F_TM'] = None
            params['F_DATATYPENAME'] = dataInfo['F_DATATYPENAME']
            params['F_PRODUCTLEVEL'] = dataInfo['F_PRODUCTLEVEL']
            params['F_IMAGEURL'] = '/mj/metaImage/getImageByTypeForAll?typeId=2&dataId={}&nodeId={}'.format(dataInfo['F_DID'],dataInfo['NODEID'])
            params['F_WKTRESPONSE'] = dataInfo['WKTRESPONSE']
            params['F_NODENAME'] = NodeIdToNodeName[dataInfo['NODEID']]
            params['F_DOCNUM_OLD'] = None
            params['F_CLOUDPERCENT'] = float(dataInfo['F_CLOUDPERCENT'])
            params['F_LOCATION'] = dataInfo['F_LOCATION']
            params['F_SGTABLENAME'] = dataInfo['F_TABLENAME']
            params['F_DID'] = int(dataInfo['F_DID'])
            params['F_ORBITID'] = None if dataInfo['F_ORBITID'] == 'None' else int(dataInfo['F_ORBITID'])
            params['F_SCENEPATH'] = dataInfo['F_SCENEPATH']
            params['F_SCENEROW'] = dataInfo['F_SCENEROW']
            params['F_SYSTEMTYPE'] = None
            executeNonQuery(pool, sql, params)
        except Exception as e:
            logger.error('添加数据到购物车失败: {}, 错误数据: {}, userid: {}'.format(e, dataInfo, userid))
    
    executor = ThreadPoolExecutor()
    executor.map(addSingleDataToShop, dataInfos, [userid]*len(dataInfos))       
       
def getShapelyAreaByCode(areacode: str, pool): 
    """根据传入的行政区划代码获取行政区划的几何形状

    Args:
        areacode (str): 行政区划的代码
        pool (_type_): 数据库连接池

    Returns:
        shapely.geometry对象: 行政区划的几何形状
    """
    try:   
        sql = 'SELECT SDO_GEOMETRY.get_wkt(GEOM) FROM TC_DISTRICT WHERE F_DISTCODE = :areacode'
        res = executeQuery(pool, sql, {'areacode': areacode})[0][0]
        geodbhandler = GeoDBHandler()
        return geodbhandler.sdoGeometryWktToShapely(res)
    except Exception as e:
        logger.error(f'无法从areacode获取对应几何形状: {e}')
        return None 

def formatDictForView(dictList: list):
    """将字典列表中的键值对进行处理, 使其适合前端展示

    Args:
        dictList (list): 字典列表

    Returns:
        list: 处理后的字典列表
    """
    try:
        res = []
        for index, data in enumerate(dictList):
            newDict = data.copy()
            
            if 'geometry' in newDict.keys():
                newDict['WKTRESPONSE'] = newDict.pop('geometry')
            try:
                newDict['NODEID'] = satelliteToNodeId[newDict['F_SATELLITEID']][newDict['F_SENSORID']]
            except Exception as e:
                logger.error(f"获取NODEID失败, 不识别的卫星名或传感器名: {e}")

            newDict['RN'] = index + 1
            newDict['NODENAME'] = NodeIdToNodeName[newDict['NODEID']]
            newDict['F_CLOUDPERCENT'] = int(float(newDict['F_CLOUDPERCENT']))
            res.append(newDict)
        return res
    except Exception as e:
        logger.error(f'格式化字典列表失败: {e}')
        return None

def generateSqlQuery(dataname: list, tablename: list, wheresql: str = None) -> str:
    """根据传入的表名和字段名生成查询语句

    Args:
        dataname (list): 传入的字段名列表
        tablename (list): 传入的表名列表

    Returns:
        str: 查询语句
    """
    try:
        tableSqlList = []
        for table in tablename:
            table = table.upper()
            columns = ','.join([f'{name}' for name in dataname])
            tableSqlList.append(f'select /*+ PARALLEL(16) */ {columns} FROM {table} {wheresql}')
        sql = ' UNION ALL '.join(tableSqlList)
        return sql
    except Exception as e:
        logger.error(f'查询语句生成失败: {e}')
        return None