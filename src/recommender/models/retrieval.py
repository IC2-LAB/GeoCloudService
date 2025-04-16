import faiss
import numpy as np
from typing import Dict, List, Tuple
import torch

class FAISSRetrieval:
    def __init__(self, dimension: int):
        """初始化FAISS检索器
        
        Args:
            dimension: 向量维度
        """
        # 使用L2距离的索引
        self.index = faiss.IndexFlatL2(dimension)
        self.id_to_idx = {}  # 数据ID到索引的映射
        self.idx_to_id = {}  # 索引到数据ID的映射
        
    def build_index(self, item_embeddings: Dict[str, np.ndarray]):
        """构建索引
        
        Args:
            item_embeddings: Dict[数据ID, 向量表示]
        """
        # 重置索引
        self.index = faiss.IndexFlatL2(item_embeddings[list(item_embeddings.keys())[0]].shape[0])
        self.id_to_idx = {}
        self.idx_to_id = {}
        
        # 构建向量矩阵
        vectors = []
        for idx, (item_id, embedding) in enumerate(item_embeddings.items()):
            vectors.append(embedding)
            self.id_to_idx[item_id] = idx
            self.idx_to_id[idx] = item_id
            
        vectors = np.stack(vectors)
        
        # 添加到索引
        self.index.add(vectors)
        
    def search(self, query_vector: np.ndarray, k: int = 10) -> List[Tuple[str, float]]:
        """搜索最相似的k个物品
        
        Args:
            query_vector: 查询向量
            k: 返回的最相似物品数量
            
        Returns:
            List[Tuple[数据ID, 相似度分数]]
        """
        # 确保查询向量是2D的
        if len(query_vector.shape) == 1:
            query_vector = query_vector.reshape(1, -1)
            
        # 搜索最近邻
        distances, indices = self.index.search(query_vector, k)
        
        # 转换索引为数据ID
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx != -1:  # FAISS可能返回-1表示未找到足够的邻居
                item_id = self.idx_to_id[idx]
                similarity = 1.0 / (1.0 + dist)  # 将L2距离转换为相似度分数
                results.append((item_id, similarity))
                
        return results
    
    def batch_search(self, query_vectors: np.ndarray, k: int = 10) -> List[List[Tuple[str, float]]]:
        """批量搜索最相似的k个物品
        
        Args:
            query_vectors: 查询向量矩阵 [batch_size, dimension]
            k: 每个查询返回的最相似物品数量
            
        Returns:
            List[List[Tuple[数据ID, 相似度分数]]]
        """
        # 搜索最近邻
        distances, indices = self.index.search(query_vectors, k)
        
        # 转换结果
        batch_results = []
        for batch_distances, batch_indices in zip(distances, indices):
            results = []
            for dist, idx in zip(batch_distances, batch_indices):
                if idx != -1:
                    item_id = self.idx_to_id[idx]
                    similarity = 1.0 / (1.0 + dist)
                    results.append((item_id, similarity))
            batch_results.append(results)
            
        return batch_results 