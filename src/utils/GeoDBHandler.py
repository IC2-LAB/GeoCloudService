import geopandas as gpd
import shapely
import numpy as np
import pandas as pd
from src.config import config
from src.utils.logger import logger

        
class GeoDBHandler:
    def __init__(self):
        self.crs = config.CRS
        
    def wktToShapely(self, wkt: str) -> gpd.GeoDataFrame:
        """将wkt转成Shapeley对象;
        Args:
            wkt: WKT格式的几何形状;
        Returns:
            gpd.GeoDataFrame;
        """
        geometry = shapely.wkt.loads(wkt)
        return geometry
        
    def dbDataToGeoDataFrame(self, rows, columns) -> gpd.GeoDataFrame:
        """将数据库查询结果转换为GeoDataFrame;
        Args:
        
            rows: 数据库查询结果;
            columns: 除几何数据外的列名;
            默认最后一列为SDO_GEOMETRY对象;
        Returns:
            gpd.GeoDataFrame;
        """
        geometries = []
        attributes = []
        for row in rows:
            # 分离几何和属性
            geometry = self.sdoGeometryWktToShapely(row[-1])
            attribute = row[:-1]
            geometries.append(geometry)
            attributes.append(attribute)
        
        # 创建GeoDataFrame
        gdf = gpd.GeoDataFrame(attributes, columns=columns, geometry=geometries, crs=self.crs)
        return gdf
    
    def imageDataToGeoDataFrame(self, rows, columns) -> gpd.GeoDataFrame:
        """将数据库查询到的遥感影像信息转化为GeoDataFrame;

        rows: 数据库查询结果;
            columns: 除几何数据外的列名;
            默认最后一列为SDO_GEOMETRY对象;
        Returns:
            gpd.GeoDataFrame;
        """

        imageData = pd.DataFrame(rows, columns=columns+['geometry'])
        geoList = imageData['geometry']
        def SdoToList(item):
            if item is not None and item.SDO_GTYPE == 2003:
                return item.SDO_ORDINATES.aslist()
            else:
                return [0] * 10
        geoList = list(map(SdoToList, geoList))
        ordinates = np.array(geoList, dtype=np.float64)
        ordinates = ordinates.reshape(-1, 5, 2)
        imageData['geometry'] = shapely.polygons(ordinates)
        # 创建GeoDataFrame
        gdf = gpd.GeoDataFrame(imageData, geometry=imageData['geometry'], crs=self.crs)
        return gdf

    def sdoGeometryToGeoDataFrame(self, sdo_geometry, additional_columns=None, additional_data=None) -> gpd.GeoDataFrame:
        """将SDO_GEOMETRY对象以及其余可能需要的属性转换为GeoDataFrame;
        
        Args:
            sdo_geometry: SDO_GEOMETRY对象,一般为从数据库中读取的数据;
            additional_columns: 其余数据的列名,如F_ID, NAME等;
            additional_data: 其余数据;
            注意additional_data和additional_columns的列数应该相等,且其行数与sdo_geometry的行数相等;
        Returns:
            gpd.GeoDataFrame;
        """
        try:
            geometries = []
            for data in sdo_geometry:
                geometry = self.sdoGeometryWktToShapely(data)
                geometries.append(geometry)
                
            if additional_columns is None and additional_data is None:
                gdf = gpd.GeoDataFrame(geometry=geometries, crs=self.crs)
            
            else:
                if len(additional_columns) != len(additional_data[0]):
                    logger.error("additional_columns和additional_data的列数不相等!")
                    return None
                if len(additional_data) != len(sdo_geometry):
                    logger.error("sdo_geometry和additional_data的行数不相等!")
                    return None
                
                gdf = gpd.GeoDataFrame(additional_data, columns=additional_columns, geometry=geometries, crs=self.crs)
            return gdf
        except Exception as e:
            logger.error(f"将SDO_GEOMETRY对象转换为GeoDataFrame时出现错误: {e}")
            return None
            
    def sdoGeometryWktToShapely(self, sdo_geometry):
        """将SDO_GEOMETRY对象转换成的wkt clob包转换为shapely几何对象。
        
        Args:
        sdo_geometry: SDO_GEOMETRY的CLOB对象(从数据库中直接读取);
        Returns:
        shapely几何对象;
        """
        
        if sdo_geometry is None:
            return None
        wkt = sdo_geometry.read()
        return shapely.wkt.loads(wkt)
    
    def sdoGeometryPolygonToShapely(self, sdo_geometry):    
        """将SDO_GEOMETRY对象的polygon类型对象转换为shapely几何对象。
        
        Args:
        sdo_geometry: SDO_GEOMETRY的CLOB对象(从数据库中直接读取);
        Returns:
        shapely几何对象;
        """
        
        if sdo_geometry is None:
            return None

        # 获取SDO_GTYPE, SDO_SRID, SDO_POINT, SDO_ELEM_INFO, SDO_ORDINATES
        sdo_gtype = sdo_geometry.SDO_GTYPE
        sdo_ordinates = sdo_geometry.SDO_ORDINATES.aslist()

        # 根据SDO_GTYPE确定几何类型
        if  sdo_gtype == 2003:  # 多边形
            polygon = shapely.geometry.Polygon(self.pairwise(sdo_ordinates))
            return polygon
        else:
            logger.error(f"不支持的类型 SDO_GTYPE: {sdo_gtype}")
            return None
        
        
    def pairwise(self, iterable):
        """将可迭代对象两两配对;
        [long_1, lat_1, long_2, lat_2 ... long_5, lat_5] -> [(long_1, lat_1), (long_2, lat_2)...]
        """
        return [(iterable[i], iterable[i + 1]) for i in range(0, len(iterable)-1, 2)]