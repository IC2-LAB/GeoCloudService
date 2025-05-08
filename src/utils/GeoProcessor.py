import geopandas as gpd
from src.config import config
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from shapely.geometry import Point, LineString, Polygon, box
from src.utils.logger import logger

class GeoProcessor:
    def __init__(self):
        self.crs = config.CRS
   
    def findIntersectedData(self, target_area, data_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        查找与目标区域相交的数据;
        target_area: 目标区域,一般为shapely.geometry对象;
        data_gdf: 数据GeoDataFrame;
        数据要求: data_gdf中至少有一个geometry列,且该列储存的是shapely.geometry对象;
        返回值: 与目标区域相交的数据GeoDataFrame;
        """
        try:
            if not isinstance(target_area, (gpd.GeoSeries, gpd.GeoDataFrame)):
                target_gdf = gpd.GeoDataFrame(geometry=[target_area], crs=data_gdf.crs)
            else:
                target_gdf = target_area
            # 使用空间连接(sjoin)进行高效的空间索引查询
            # 'predicate'参数替代了旧版的'op'参数
            intersecting_gdf = gpd.sjoin(
                data_gdf,
                target_gdf,
                how='inner',
                predicate='intersects'
            )
            # 去除sjoin自动添加的右表索引列，返回原数据的子集
            return intersecting_gdf.drop(columns=['index_right']).copy()
        except Exception as e:
            logger.error(f"检查相交数据时出现错误: {e}")
            return gpd.GeoDataFrame()

    def rmHighlyOverlappingData(self, geo_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """去除高度重叠的数据;

        Args:
            geo_df (gpd.GeoDataFrame): 需过滤的GeoDataFrame数据

        Returns:
            gpd.GeoDataFrame: 过滤后的GeoDataFrame数据
        """
        # 计算每条数据的覆盖区域面积
        areas = geo_df.geometry.area
        # 初始化一个列表来存储高度重叠的数据索引
        highly_overlapping_indices = []
        
        # 使用空间索引加速空间查询
        sindex = geo_df.geometry.sindex

        def check_overlap(args):
            item, sindex, areas, geo_df = args
            idx , geom = item
            # 查找可能包含当前地理数据的其他地理数据的索引
            possible_overlapping_indices = list(sindex.intersection(geom.bounds))
            # 获取可能包含当前地理数据的其他地理数据
            possible_overlapping_geoms = geo_df.geometry.iloc[possible_overlapping_indices]
            # 检查当前数据是否完全被其他数据的并集包含
            if geom.within(possible_overlapping_geoms.unary_union):
                # 检查当前数据是否不是最大的覆盖区域
                if geom.area < areas.max():
                    return idx
            return None
        
        # 使用多线程并行化计算
        with ThreadPoolExecutor() as executor:
            results = executor.map(check_overlap, zip(geo_df.geometry.items(), repeat(sindex), repeat(areas), repeat(geo_df)))
        
        # 收集结果
        for result in results:
            if result is not None:
                highly_overlapping_indices.append(result)
        
        # 去除高度重叠的数据
        filtered_geo_df = geo_df.drop(highly_overlapping_indices)
        return filtered_geo_df
        
    def calCoverageRatio(self,target_area, data_gdf: gpd.GeoDataFrame) -> float:
        """
        计算数据覆盖率;
        target_area: 目标区域,一般为shapely.geometry.Polygon对象;
        data_gdf: 数据GeoDataFrame;
        数据要求: data_gdf中至少有一个geometry列,且该列储存的是shapely.geometry.Polygon对象;
        """
        try:
            # 合并data_gdf中的几何形状
            combined_data = data_gdf['geometry'].unary_union
            
            # 计算相交区域
            intersection = combined_data.intersection(target_area)
            
            # 计算覆盖率
            coverage_ratio = 0

            if intersection.is_empty:
                return 0.0
            
            if isinstance(target_area, Point):
                coverage_ratio = 1
            elif isinstance(target_area, LineString):
                coverage_ratio = intersection.length / target_area.length
            else:
                coverage_ratio = intersection.area / target_area.area
            
            return coverage_ratio
        except Exception as e:
            logger.error(f"计算覆盖率时出现错误: {e}")
            return 0.0

    def getEnvelope(self, data: gpd.GeoDataFrame) -> Polygon:
        """
        获取数据的外接矩形;
        data: GeoDataFrame;
        返回值: 外接矩形,一般为shapely.geometry.Polygon对象;
        """
        try:
            
            minx, miny, maxx, maxy = data.total_bounds
            return box(minx, miny, maxx, maxy)
        except Exception as e:
            logger.error(f"获取外接矩形时出现错误: {e}")
            return None
        
    def getCoordinateRange(self, geom) -> tuple:
        """
        获取几何形状的坐标范围;
        geom: shapely.geometry对象;
        返回值: (minx, maxx, miny, maxy);
        """
        try:
            minx, miny, maxx, maxy = geom.bounds
            return (minx, maxx, miny, maxy)
        except Exception as e:
            logger.error(f"获取坐标范围时出现错误: {e}")
            return None



    def GeoDataFrameToDict(self, data: gpd.GeoDataFrame) -> list:
        """
        将GeoDataFrame转换为字典列表;
        data: GeoDataFrame;
        返回值格式：[{'dataname': 'A', 'geometry': 'POLYGON ((...))'}, ...]
        """
        # 创建副本避免修改原数据
        df = data.copy()
        
        # 批量转换几何列为WKT字符串，处理空值
        df['geometry'] = df.geometry.apply(lambda g: g.wkt if g is not None else None)
        
        # 批量将非几何列转换为字符串类型
        non_geom_cols = df.columns[df.columns != 'geometry']
        df[non_geom_cols] = df[non_geom_cols].astype(str)
        
        # 直接转换为记录字典列表
        return df.to_dict(orient='records')

    def GeoDataFrameToList(self, data: gpd.GeoDataFrame) -> list:
        """
        将GeoDataFrame转换为列表;
        data: GeoDataFrame;
        返回值格式：[['A', 'POLYGON ((...))'], ['B', 'POLYGON ((...))'], ...]
        """
        result_list = []
        for _, row in data.iterrows():
            wkt = row['geometry'].wkt
            other_values = [str(value) for key, value in row.items() if key != 'geometry']
            result_list.append(other_values + [wkt])
        
        return result_list

    def calculateMergedArea(self, data_gdf: gpd.GeoDataFrame) -> dict:
        """
        计算数据的合并面及其总面积。
        Args:
            data_gdf (GeoDataFrame): 包含几何数据的GeoDataFrame。
        Returns:
            dict: 合并面WKT和总面积
        """
        try:
            # 合并所有几何形状
            combined_data = data_gdf['geometry'].unary_union

            # 计算总面积
            total_area = combined_data.area

            # 返回合并面和面积
            return combined_data.wkt, total_area
        except Exception as e:
            logger.error(f"计算合并面及其面积时出现错误: {e}")
            return None