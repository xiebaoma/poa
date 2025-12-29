# -*- coding: utf-8 -*-
"""
Spark 数据处理模块

使用 PySpark 进行大规模文本数据处理
"""
import os
import sys
from typing import List, Dict, Optional
import pandas as pd

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# 自动设置 JAVA_HOME（如果未设置）
if not os.environ.get('JAVA_HOME'):
    java_path = "D:\java"
    if os.path.exists(java_path):
        os.environ['JAVA_HOME'] = java_path

# PySpark 导入 - Windows 可能有兼容性问题
PYSPARK_AVAILABLE = False
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, explode, split, lower, trim, 
        count, desc, udf, collect_list, avg
    )
    from pyspark.sql.types import StringType, ArrayType, FloatType
    PYSPARK_AVAILABLE = True
except Exception as e:
    print(f"提示: PySpark 不可用 ({e})，将使用 Pandas 进行数据处理")


class SparkProcessor:
    """Spark 数据处理器"""
    
    def __init__(self, app_name: str = None, master: str = None):
        """
        初始化 Spark 处理器
        
        Args:
            app_name: Spark 应用名称
            master: Spark master URL (默认为本地模式)
        """
        self.app_name = app_name or config.SPARK_APP_NAME
        self.master = master or config.SPARK_MASTER
        self.spark = None
        self._initialized = False
    
    def init_spark(self) -> bool:
        """
        初始化 Spark Session
        
        Returns:
            是否初始化成功
        """
        if not PYSPARK_AVAILABLE:
            print("PySpark 不可用，跳过 Spark 初始化")
            return False
        
        if self._initialized and self.spark is not None:
            return True
        
        try:
            print(f"正在初始化 Spark Session ({self.master})...")
            
            # 设置环境变量（Windows 兼容性）
            os.environ['PYSPARK_PYTHON'] = sys.executable
            os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
            
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master) \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.sql.shuffle.partitions", "8") \
                .config("spark.ui.enabled", "false") \
                .getOrCreate()
            
            # 设置日志级别
            self.spark.sparkContext.setLogLevel("ERROR")
            
            self._initialized = True
            print("Spark Session 初始化成功")
            return True
            
        except Exception as e:
            print(f"Spark 初始化失败: {e}")
            self._initialized = False
            return False
    
    def stop(self):
        """停止 Spark Session"""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
            self._initialized = False
            print("Spark Session 已停止")
    
    def load_dataframe(self, df: pd.DataFrame):
        """
        将 Pandas DataFrame 转换为 Spark DataFrame
        
        Args:
            df: Pandas DataFrame
        
        Returns:
            Spark DataFrame 或 None
        """
        if not self._initialized:
            if not self.init_spark():
                return None
        
        try:
            spark_df = self.spark.createDataFrame(df)
            return spark_df
        except Exception as e:
            print(f"转换 DataFrame 失败: {e}")
            return None
    
    def process_word_frequency(self, df: pd.DataFrame, 
                                tokenize_func,
                                group_by_date: bool = True) -> pd.DataFrame:
        """
        使用 Spark 计算词频
        
        Args:
            df: 包含 'content' 和 'date' 列的 DataFrame
            tokenize_func: 分词函数，接收文本返回词列表
            group_by_date: 是否按日期分组
        
        Returns:
            词频 DataFrame
        """
        if not PYSPARK_AVAILABLE or not self.init_spark():
            return self._process_word_frequency_pandas(df, tokenize_func, group_by_date)
        
        try:
            print("使用 Spark 进行词频统计...")
            
            # 先在 Python 端完成分词（避免 UDF 在 Windows 上崩溃）
            print("  - Python端分词处理...")
            word_records = []
            for _, row in df.iterrows():
                content = row.get('content', '')
                date = row.get('date', 'unknown')
                
                if pd.isna(content) or not content:
                    continue
                
                words = tokenize_func(str(content))
                for word in words:
                    word_records.append({'date': str(date), 'word': word})
            
            if not word_records:
                return pd.DataFrame(columns=['date', 'word', 'count'] if group_by_date else ['word', 'count'])
            
            # 转换为 Spark DataFrame 进行聚合
            print("  - Spark聚合统计...")
            word_df = pd.DataFrame(word_records)
            spark_df = self.spark.createDataFrame(word_df)
            
            # 使用 Spark 进行分组聚合
            if group_by_date:
                result_df = spark_df.groupBy("date", "word") \
                    .agg(count("*").alias("count")) \
                    .orderBy(desc("count"))
            else:
                result_df = spark_df.groupBy("word") \
                    .agg(count("*").alias("count")) \
                    .orderBy(desc("count"))
            
            # 转回 Pandas
            print("  - Spark处理完成")
            return result_df.toPandas()
            
        except Exception as e:
            print(f"Spark 处理失败，使用 Pandas: {e}")
            return self._process_word_frequency_pandas(df, tokenize_func, group_by_date)
    
    def _process_word_frequency_pandas(self, df: pd.DataFrame,
                                        tokenize_func,
                                        group_by_date: bool = True) -> pd.DataFrame:
        """使用 Pandas 计算词频（后备方案）"""
        print("使用 Pandas 进行词频统计...")
        
        results = []
        
        for _, row in df.iterrows():
            content = row.get('content', '')
            date = row.get('date', 'unknown')
            
            if pd.isna(content) or not content:
                continue
            
            words = tokenize_func(str(content))
            
            for word in words:
                if group_by_date:
                    results.append({'date': date, 'word': word, 'count': 1})
                else:
                    results.append({'word': word, 'count': 1})
        
        if not results:
            return pd.DataFrame(columns=['date', 'word', 'count'] if group_by_date else ['word', 'count'])
        
        result_df = pd.DataFrame(results)
        
        if group_by_date:
            result_df = result_df.groupby(['date', 'word']).sum().reset_index()
        else:
            result_df = result_df.groupby(['word']).sum().reset_index()
        
        result_df = result_df.sort_values('count', ascending=False)
        
        return result_df
    
    def get_top_words_by_date(self, word_freq_df: pd.DataFrame, 
                              top_n: int = None) -> Dict[str, List[tuple]]:
        """
        获取每日 Top N 热词
        
        Args:
            word_freq_df: 词频 DataFrame (包含 date, word, count 列)
            top_n: 每日热词数量
        
        Returns:
            {date: [(word, count), ...], ...}
        """
        top_n = top_n or config.DEFAULT_TOP_N
        
        result = {}
        
        if 'date' not in word_freq_df.columns:
            # 没有日期，返回总体 Top N
            top_words = word_freq_df.nlargest(top_n, 'count')
            result['all'] = list(zip(top_words['word'], top_words['count']))
            return result
        
        # 按日期分组获取 Top N
        for date in word_freq_df['date'].unique():
            date_df = word_freq_df[word_freq_df['date'] == date]
            top_words = date_df.nlargest(top_n, 'count')
            result[str(date)] = list(zip(top_words['word'], top_words['count']))
        
        return result


# 全局处理器实例
_processor = None

def get_processor() -> SparkProcessor:
    """获取全局 Spark 处理器实例"""
    global _processor
    if _processor is None:
        _processor = SparkProcessor()
    return _processor


if __name__ == "__main__":
    # 测试 Spark 处理器
    print("=" * 50)
    print("测试 Spark 处理器")
    print("=" * 50)
    
    processor = get_processor()
    
    if processor.init_spark():
        print("Spark 初始化成功!")
        
        # 创建测试数据
        test_data = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-01', '2024-01-02'],
            'content': ['今天天气真好', '天气不错，心情好', '明天天气怎么样']
        })
        
        # 简单分词函数（测试用）
        def simple_tokenize(text):
            import jieba
            return list(jieba.cut(text))
        
        # 测试词频统计
        result = processor.process_word_frequency(test_data, simple_tokenize)
        print("\n词频统计结果:")
        print(result)
        
        processor.stop()
    else:
        print("Spark 初始化失败，测试跳过")
