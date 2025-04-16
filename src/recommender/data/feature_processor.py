import tensorflow as tf
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

class FeatureProcessor:
    """特征处理器，用于处理用户特征并创建TensorFlow特征列"""
    
    def __init__(self):
        self.user_vocab = None
        self.numeric_features = []
        self.categorical_features = []
        self.feature_columns = []
        self.feature_embeddings = {}  # 存储特征embedding层
        
    def process_features(self, features_df: pd.DataFrame) -> None:
        """处理特征DataFrame，创建特征列
        
        Args:
            features_df: 包含用户特征的DataFrame
        """
        # 获取数值型和类别型特征
        numeric_features = features_df.select_dtypes(include=[np.number]).columns
        categorical_features = features_df.select_dtypes(exclude=[np.number]).columns
        
        self.numeric_features = list(numeric_features)
        self.categorical_features = list(categorical_features)
        
        # 创建数值型特征列
        for feature_name in numeric_features:
            # 标准化数值特征
            mean = features_df[feature_name].mean()
            std = features_df[feature_name].std()
            normalizer = lambda x: (x - mean) / (std + 1e-7)
            
            column = tf.feature_column.numeric_column(
                feature_name,
                normalizer_fn=normalizer
            )
            self.feature_columns.append(column)
            
        # 创建类别型特征列
        for feature_name in categorical_features:
            # 获取类别词汇表
            vocab = features_df[feature_name].unique().tolist()
            
            # 创建类别型特征列
            column = tf.feature_column.categorical_column_with_vocabulary_list(
                feature_name,
                vocab
            )
            
            # 转换为embedding列
            embedding_dim = min(len(vocab) // 2, 50)  # embedding维度启发式设置
            embedding_column = tf.feature_column.embedding_column(
                column,
                dimension=embedding_dim
            )
            self.feature_columns.append(embedding_column)
            
            # 保存embedding层信息
            self.feature_embeddings[feature_name] = {
                'vocab': vocab,
                'embedding_dim': embedding_dim
            }
            
    def create_user_vocab(self, user_ids: List[str]) -> None:
        """创建用户ID词汇表
        
        Args:
            user_ids: 用户ID列表
        """
        # 创建词汇表（包含特殊token）
        self.user_vocab = ['<PAD>', '<UNK>'] + sorted(set(user_ids))
        
    def get_user_vocab_size(self) -> int:
        """获取用户词汇表大小"""
        if self.user_vocab is None:
            raise ValueError("请先调用create_user_vocab创建用户词汇表")
        return len(self.user_vocab)
    
    def convert_to_user_id(self, user: str) -> int:
        """将用户名转换为ID
        
        Args:
            user: 用户名
            
        Returns:
            用户ID（词汇表索引）
        """
        if self.user_vocab is None:
            raise ValueError("请先调用create_user_vocab创建用户词汇表")
            
        try:
            return self.user_vocab.index(user)
        except ValueError:
            return 1  # 返回<UNK>的索引
            
    def create_tf_dataset(
        self,
        features_df: pd.DataFrame,
        batch_size: int = 32,
        shuffle: bool = True
    ) -> tf.data.Dataset:
        """创建TensorFlow数据集
        
        Args:
            features_df: 特征DataFrame
            batch_size: 批次大小
            shuffle: 是否打乱数据
            
        Returns:
            TensorFlow数据集
        """
        # 创建特征字典
        feature_dict = {}
        
        # 添加用户ID特征
        user_ids = [self.convert_to_user_id(user) for user in features_df.index]
        feature_dict['user_id'] = np.array(user_ids)
        
        # 添加其他特征
        for feature in self.numeric_features + self.categorical_features:
            feature_dict[feature] = features_df[feature].values
            
        # 创建数据集
        dataset = tf.data.Dataset.from_tensor_slices(feature_dict)
        
        if shuffle:
            dataset = dataset.shuffle(buffer_size=len(features_df))
            
        dataset = dataset.batch(batch_size)
        
        return dataset
        
    def get_feature_columns(self) -> List[tf.feature_column.FeatureColumn]:
        """获取特征列"""
        return self.feature_columns

    def get_user_embedding(
        self,
        user_features: Dict[str, np.ndarray],
        feature_weights: Optional[Dict[str, float]] = None
    ) -> tf.Tensor:
        """获取用户特征向量表示
        
        Args:
            user_features: 用户特征字典，key为特征名，value为特征值
            feature_weights: 特征权重字典，用于加权组合不同特征
            
        Returns:
            用户特征向量
        """
        if not self.feature_columns:
            raise ValueError("请先调用process_features处理特征")
            
        # 创建特征输入层
        feature_inputs = {}
        for feature_name in user_features:
            feature_inputs[feature_name] = tf.keras.Input(shape=(1,), name=feature_name)
            
        # 创建特征层
        feature_layers = []
        for feature_name, feature_value in user_features.items():
            if feature_name in self.numeric_features:
                # 数值特征直接使用
                feature_layer = tf.keras.layers.Dense(1)(feature_inputs[feature_name])
            else:
                # 类别特征使用embedding
                vocab = self.feature_embeddings[feature_name]['vocab']
                embedding_dim = self.feature_embeddings[feature_name]['embedding_dim']
                
                # 创建embedding层
                embedding_layer = tf.keras.layers.Embedding(
                    input_dim=len(vocab),
                    output_dim=embedding_dim,
                    name=f"{feature_name}_embedding"
                )
                feature_layer = embedding_layer(feature_inputs[feature_name])
                
            feature_layers.append(feature_layer)
            
        # 特征组合
        if feature_weights:
            # 加权组合
            weighted_features = []
            for i, feature_name in enumerate(user_features):
                weight = feature_weights.get(feature_name, 1.0)
                weighted_features.append(feature_layers[i] * weight)
            combined_features = tf.keras.layers.Concatenate()(weighted_features)
        else:
            # 简单拼接
            combined_features = tf.keras.layers.Concatenate()(feature_layers)
            
        # 创建模型
        model = tf.keras.Model(inputs=feature_inputs, outputs=combined_features)
        
        # 获取用户向量
        user_vector = model(user_features)
        
        return user_vector