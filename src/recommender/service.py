import torch
import numpy as np
from typing import List, Dict, Any
import oracledb
from .feature.user_feature import UserFeatureExtractor
from .feature.item_feature import ItemFeatureExtractor
from .model.tower_model import TwoTowerModel
from .model.retrieval import FAISSRetrieval

class RecommenderService:
    def __init__(self, conn, model_path: str = None):
        """初始化推荐服务
        
        Args:
            conn: Oracle数据库连接
            model_path: 模型保存路径
        """
        self.conn = conn
        self.user_feature_extractor = UserFeatureExtractor(conn)
        self.item_feature_extractor = ItemFeatureExtractor(conn)
        
        # 加载特征
        self.user_features = self.user_feature_extractor.extract_user_features()
        self.item_features = self.item_feature_extractor.extract_item_features()
        
        # 创建ID映射
        self.user_id_to_idx = {uid: idx for idx, uid in enumerate(self.user_features.keys())}
        self.satellite_type_to_idx = {}
        self.region_to_idx = {}
        
        # 收集唯一的卫星类型和地区
        for features in self.item_features.values():
            if features["satellite_type"] not in self.satellite_type_to_idx:
                self.satellite_type_to_idx[features["satellite_type"]] = len(self.satellite_type_to_idx)
            if features["region_code"] not in self.region_to_idx:
                self.region_to_idx[features["region_code"]] = len(self.region_to_idx)
        
        # 初始化模型
        self.model = TwoTowerModel(
            num_users=len(self.user_features),
            num_satellites=len(self.satellite_type_to_idx),
            num_regions=len(self.region_to_idx)
        )
        
        if model_path:
            self.model.load_state_dict(torch.load(model_path))
        self.model.eval()
        
        # 初始化FAISS检索器
        self.retrieval = FAISSRetrieval(dimension=64)  # 使用模型的输出维度
        self._build_item_index()
        
    def _build_item_index(self):
        """构建物品索引"""
        with torch.no_grad():
            item_embeddings = {}
            for item_id, features in self.item_features.items():
                # 准备输入特征
                satellite_id = torch.tensor([self.satellite_type_to_idx[features["satellite_type"]]])
                region_id = torch.tensor([self.region_to_idx[features["region_code"]]])
                continuous_features = torch.tensor([[
                    features["resolution"],
                    features["cloud_cover"],
                    features["popularity_score"]
                ]])
                
                # 获取物品向量表示
                item_emb = self.model.get_item_embedding(
                    satellite_id, region_id, continuous_features
                ).numpy()
                
                item_embeddings[item_id] = item_emb[0]
                
            # 构建FAISS索引
            self.retrieval.build_index(item_embeddings)
    
    def get_recommendations(self, user_id: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """获取用户推荐结果
        
        Args:
            user_id: 用户ID
            top_k: 推荐结果数量
            
        Returns:
            List[Dict]: 推荐结果列表，每个结果包含数据ID和相似度分数
        """
        if user_id not in self.user_id_to_idx:
            return []
        
        with torch.no_grad():
            # 获取用户向量表示
            user_idx = torch.tensor([self.user_id_to_idx[user_id]])
            user_embedding = self.model.get_user_embedding(user_idx).numpy()
            
            # 检索相似物品
            similar_items = self.retrieval.search(user_embedding[0], k=top_k)
            
            # 构建推荐结果
            recommendations = []
            for item_id, similarity in similar_items:
                item_info = self.item_features[item_id].copy()
                item_info["data_id"] = item_id
                item_info["similarity_score"] = float(similarity)
                recommendations.append(item_info)
                
            return recommendations
    
    def batch_get_recommendations(self, user_ids: List[str], top_k: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """批量获取用户推荐结果
        
        Args:
            user_ids: 用户ID列表
            top_k: 每个用户的推荐结果数量
            
        Returns:
            Dict[用户ID, 推荐结果列表]
        """
        valid_users = [uid for uid in user_ids if uid in self.user_id_to_idx]
        if not valid_users:
            return {}
        
        with torch.no_grad():
            # 获取用户向量表示
            user_indices = torch.tensor([self.user_id_to_idx[uid] for uid in valid_users])
            user_embeddings = self.model.get_user_embedding(user_indices).numpy()
            
            # 批量检索相似物品
            batch_similar_items = self.retrieval.batch_search(user_embeddings, k=top_k)
            
            # 构建推荐结果
            recommendations = {}
            for uid, similar_items in zip(valid_users, batch_similar_items):
                user_recs = []
                for item_id, similarity in similar_items:
                    item_info = self.item_features[item_id].copy()
                    item_info["data_id"] = item_id
                    item_info["similarity_score"] = float(similarity)
                    user_recs.append(item_info)
                recommendations[uid] = user_recs
                
            return recommendations 