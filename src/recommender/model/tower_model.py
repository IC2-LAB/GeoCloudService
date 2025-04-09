import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, List, Any, Tuple

class UserTower(nn.Module):
    def __init__(self, 
                 num_users: int,
                 embedding_dim: int = 64,
                 hidden_dims: List[int] = [128, 64]):
        """用户塔
        
        Args:
            num_users: 用户总数
            embedding_dim: 嵌入维度
            hidden_dims: 隐藏层维度列表
        """
        super().__init__()
        self.user_embedding = nn.Embedding(num_users, embedding_dim)
        
        # 构建MLP层
        layers = []
        input_dim = embedding_dim
        for hidden_dim in hidden_dims:
            layers.extend([
                nn.Linear(input_dim, hidden_dim),
                nn.ReLU(),
                nn.BatchNorm1d(hidden_dim),
                nn.Dropout(0.2)
            ])
            input_dim = hidden_dim
            
        self.mlp = nn.Sequential(*layers)
        
    def forward(self, user_ids: torch.Tensor) -> torch.Tensor:
        """前向传播
        
        Args:
            user_ids: 用户ID张量
            
        Returns:
            用户表示向量
        """
        x = self.user_embedding(user_ids)
        return self.mlp(x)

class ItemTower(nn.Module):
    def __init__(self,
                 num_satellites: int,
                 num_regions: int,
                 embedding_dim: int = 64,
                 hidden_dims: List[int] = [128, 64]):
        """物品塔
        
        Args:
            num_satellites: 卫星类型数量
            num_regions: 地区数量
            embedding_dim: 嵌入维度
            hidden_dims: 隐藏层维度列表
        """
        super().__init__()
        self.satellite_embedding = nn.Embedding(num_satellites, embedding_dim)
        self.region_embedding = nn.Embedding(num_regions, embedding_dim)
        
        # 构建MLP层
        layers = []
        # 输入维度：卫星嵌入 + 地区嵌入 + 连续特征(分辨率、云量等)
        input_dim = embedding_dim * 2 + 3  
        for hidden_dim in hidden_dims:
            layers.extend([
                nn.Linear(input_dim, hidden_dim),
                nn.ReLU(),
                nn.BatchNorm1d(hidden_dim),
                nn.Dropout(0.2)
            ])
            input_dim = hidden_dim
            
        self.mlp = nn.Sequential(*layers)
        
    def forward(self, 
                satellite_ids: torch.Tensor,
                region_ids: torch.Tensor,
                continuous_features: torch.Tensor) -> torch.Tensor:
        """前向传播
        
        Args:
            satellite_ids: 卫星类型ID张量
            region_ids: 地区ID张量
            continuous_features: 连续特征张量 [分辨率, 云量, 热度得分]
            
        Returns:
            物品表示向量
        """
        sat_emb = self.satellite_embedding(satellite_ids)
        region_emb = self.region_embedding(region_ids)
        
        # 拼接所有特征
        x = torch.cat([sat_emb, region_emb, continuous_features], dim=1)
        return self.mlp(x)

class TwoTowerModel(nn.Module):
    def __init__(self,
                 num_users: int,
                 num_satellites: int,
                 num_regions: int,
                 embedding_dim: int = 64,
                 hidden_dims: List[int] = [128, 64]):
        """双塔模型
        
        Args:
            num_users: 用户总数
            num_satellites: 卫星类型数量
            num_regions: 地区数量
            embedding_dim: 嵌入维度
            hidden_dims: 隐藏层维度列表
        """
        super().__init__()
        self.user_tower = UserTower(num_users, embedding_dim, hidden_dims)
        self.item_tower = ItemTower(num_satellites, num_regions, embedding_dim, hidden_dims)
        
    def forward(self,
                user_ids: torch.Tensor,
                satellite_ids: torch.Tensor,
                region_ids: torch.Tensor,
                continuous_features: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        """前向传播
        
        Args:
            user_ids: 用户ID张量
            satellite_ids: 卫星类型ID张量
            region_ids: 地区ID张量
            continuous_features: 连续特征张量
            
        Returns:
            (用户表示向量, 物品表示向量)
        """
        user_embeddings = self.user_tower(user_ids)
        item_embeddings = self.item_tower(satellite_ids, region_ids, continuous_features)
        
        # L2归一化
        user_embeddings = F.normalize(user_embeddings, p=2, dim=1)
        item_embeddings = F.normalize(item_embeddings, p=2, dim=1)
        
        return user_embeddings, item_embeddings
    
    def get_user_embedding(self, user_ids: torch.Tensor) -> torch.Tensor:
        """获取用户表示向量
        """
        return F.normalize(self.user_tower(user_ids), p=2, dim=1)
    
    def get_item_embedding(self,
                          satellite_ids: torch.Tensor,
                          region_ids: torch.Tensor,
                          continuous_features: torch.Tensor) -> torch.Tensor:
        """获取物品表示向量
        """
        return F.normalize(
            self.item_tower(satellite_ids, region_ids, continuous_features),
            p=2, dim=1
        ) 