import tensorflow as tf
import numpy as np
from typing import Dict, List, Tuple, Any
import json
import os

class UserTower(tf.keras.Model):
    """用户塔模型
    
    包含用户ID的embedding层和用户特征的处理层
    """
    def __init__(
        self,
        user_vocab_size: int,
        embedding_dim: int = 128,
        dense_units: List[int] = [256, 128],
        feature_columns: List[tf.feature_column.FeatureColumn] = None,
        dropout_rate: float = 0.1,
    ):
        super(UserTower, self).__init__()
        
        # 用户ID embedding层
        self.user_embedding = tf.keras.layers.Embedding(
            user_vocab_size,
            embedding_dim,
            name='user_embedding'
        )
        
        # 特征处理层
        self.feature_layer = tf.keras.layers.DenseFeatures(feature_columns)
        
        # 全连接层
        self.dense_layers = []
        for units in dense_units:
            self.dense_layers.extend([
                tf.keras.layers.Dense(units, activation='relu'),
                tf.keras.layers.BatchNormalization(),
                tf.keras.layers.Dropout(dropout_rate)
            ])
            
        # 输出层
        self.output_layer = tf.keras.layers.Dense(embedding_dim)
        
    def call(self, inputs: Dict[str, tf.Tensor], training: bool = False) -> tf.Tensor:
        # 处理用户ID
        user_embedding = self.user_embedding(inputs['user_id'])
        
        # 处理特征
        features = self.feature_layer(inputs)
        
        # 连接embedding和特征
        x = tf.concat([user_embedding, features], axis=1)
        
        # 通过全连接层
        for layer in self.dense_layers:
            x = layer(x, training=training)
            
        # 输出
        return self.output_layer(x)

class TowerModelTrainer:
    """双塔模型训练器"""
    def __init__(
        self,
        user_vocab_size: int,
        embedding_dim: int = 128,
        learning_rate: float = 0.001
    ):
        self.user_tower = None
        self.user_vocab_size = user_vocab_size
        self.embedding_dim = embedding_dim
        self.learning_rate = learning_rate
        
    def build_user_tower(
        self,
        feature_columns: List[tf.feature_column.FeatureColumn],
        dense_units: List[int] = [256, 128]
    ) -> None:
        """构建用户塔"""
        self.user_tower = UserTower(
            user_vocab_size=self.user_vocab_size,
            embedding_dim=self.embedding_dim,
            dense_units=dense_units,
            feature_columns=feature_columns
        )
        
    def compile_model(self) -> None:
        """编译模型"""
        if self.user_tower is None:
            raise ValueError("请先调用build_user_tower构建用户塔")
            
        self.user_tower.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=self.learning_rate),
            loss=tf.keras.losses.CosineSimilarity()
        )
        
    def train(
        self,
        train_dataset: tf.data.Dataset,
        validation_dataset: tf.data.Dataset = None,
        epochs: int = 10,
        **kwargs
    ) -> tf.keras.callbacks.History:
        """训练模型"""
        if self.user_tower is None:
            raise ValueError("请先调用build_user_tower构建用户塔")
            
        return self.user_tower.fit(
            train_dataset,
            validation_data=validation_dataset,
            epochs=epochs,
            **kwargs
        )
        
    def save_model(self, model_dir: str) -> None:
        """保存模型"""
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)
            
        # 保存模型权重
        self.user_tower.save_weights(os.path.join(model_dir, 'user_tower_weights'))
        
        # 保存模型配置
        config = {
            'user_vocab_size': self.user_vocab_size,
            'embedding_dim': self.embedding_dim,
            'learning_rate': self.learning_rate
        }
        with open(os.path.join(model_dir, 'config.json'), 'w') as f:
            json.dump(config, f)
            
    @classmethod
    def load_model(
        cls,
        model_dir: str,
        feature_columns: List[tf.feature_column.FeatureColumn]
    ) -> 'TowerModelTrainer':
        """加载模型"""
        # 加载配置
        with open(os.path.join(model_dir, 'config.json'), 'r') as f:
            config = json.load(f)
            
        # 创建trainer实例
        trainer = cls(
            user_vocab_size=config['user_vocab_size'],
            embedding_dim=config['embedding_dim'],
            learning_rate=config['learning_rate']
        )
        
        # 构建模型
        trainer.build_user_tower(feature_columns)
        
        # 加载权重
        trainer.user_tower.load_weights(os.path.join(model_dir, 'user_tower_weights'))
        
        return trainer