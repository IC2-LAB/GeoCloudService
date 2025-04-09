import oracledb
from typing import Dict, List, Any

class UserFeatureExtractor:
    def __init__(self, conn):
        """初始化用户特征提取器
        
        Args:
            conn: Oracle数据库连接
        """
        self.conn = conn
        self.cursor = conn.cursor()
        
    def get_user_order_stats(self) -> Dict[str, Any]:
        """获取用户订单统计信息
        
        Returns:
            Dict包含:
            - total_users: 总用户数
            - ordered_users: 下单用户数
            - user_order_counts: 每个用户的订单数
        """
        # 获取总用户数（有登录行为的用户）
        self.cursor.execute("""
            SELECT COUNT(DISTINCT USER_ID) 
            FROM USER_BEHAVIOR_LOG 
            WHERE ACTION_TYPE = 'LOGIN'
        """)
        total_users = self.cursor.fetchone()[0]
        
        # 获取下单用户数
        self.cursor.execute("""
            SELECT COUNT(DISTINCT USER_ID) 
            FROM TF_ORDER
        """)
        ordered_users = self.cursor.fetchone()[0]
        
        # 获取每个用户的订单数
        self.cursor.execute("""
            SELECT USER_ID, COUNT(*) as order_count
            FROM TF_ORDER
            GROUP BY USER_ID
        """)
        user_order_counts = {row[0]: row[1] for row in self.cursor.fetchall()}
        
        return {
            "total_users": total_users,
            "ordered_users": ordered_users,
            "user_order_counts": user_order_counts
        }
    
    def get_user_preferences(self) -> Dict[str, List[str]]:
        """获取用户偏好特征
        
        Returns:
            Dict[用户ID, List[常用卫星类型]]
        """
        # 获取用户常用的卫星类型
        self.cursor.execute("""
            SELECT USER_ID, 
                   LISTAGG(SATELLITE_TYPE, ',') WITHIN GROUP (ORDER BY COUNT(*) DESC) as preferred_types
            FROM TF_ORDER
            GROUP BY USER_ID
        """)
        
        user_preferences = {}
        for row in self.cursor.fetchall():
            user_id, preferred_types = row
            user_preferences[user_id] = preferred_types.split(',')
            
        return user_preferences
    
    def extract_user_features(self) -> Dict[str, Dict[str, Any]]:
        """提取所有用户特征
        
        Returns:
            Dict[用户ID, 用户特征字典]
        """
        stats = self.get_user_order_stats()
        preferences = self.get_user_preferences()
        
        user_features = {}
        for user_id in stats["user_order_counts"].keys():
            user_features[user_id] = {
                "order_count": stats["user_order_counts"].get(user_id, 0),
                "preferred_satellites": preferences.get(user_id, []),
                "order_frequency": stats["user_order_counts"].get(user_id, 0) / 30  # 假设统计周期为30天
            }
            
        return user_features 