"""
热点话题挖掘器 - 精简版
解决 Windows 环境下复杂操作可能导致的稳定性问题
"""
from pathlib import Path
import os
import sys
import platform
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, count, desc, udf, lit
from pyspark.sql.types import ArrayType, StringType

# ==========================================
# Windows 极致稳定性补丁
# ==========================================
if platform.system() == 'Windows':
    py_exe = sys.executable.replace('\\', '/')
    os.environ['PYSPARK_PYTHON'] = py_exe
    os.environ['PYSPARK_DRIVER_PYTHON'] = py_exe
    os.environ['PYSPARK_NO_DAEMON'] = '1'
    os.environ['PYTHONUNBUFFERED'] = '1'

class TopicMiner:
    """精简版热点话题挖掘器"""
    
    def __init__(self, spark_session=None, config=None):
        self.spark = spark_session
        self.config = config or {}

    def get_top_words(self, df, tokens_column='tokens', top_n=20):
        """词频统计"""
        print(f"统计高频词 Top-{top_n}...")
        return df.select(explode(col(tokens_column)).alias('word')) \
            .groupBy('word') \
            .agg(count('*').alias('count')) \
            .orderBy(col('count').desc()) \
            .limit(top_n)

    def get_cooccurrence(self, df, tokens_column='tokens', min_count=2):
        """精简版词共现统计"""
        def generate_pairs(tokens):
            if not tokens or len(tokens) < 2: return []
            unique_tokens = sorted(list(set(tokens)))
            pairs = []
            for i in range(len(unique_tokens)):
                for j in range(i + 1, len(unique_tokens)):
                    pairs.append(f"{unique_tokens[i]}|{unique_tokens[j]}")
            return pairs[:100]
        
        pairs_udf = udf(generate_pairs, ArrayType(StringType()))
        
        print("计算词共现关系...")
        return df.withColumn('pairs', pairs_udf(col(tokens_column))) \
            .select(explode(col('pairs')).alias('pair')) \
            .groupBy('pair') \
            .agg(count('*').alias('count')) \
            .filter(col('count') >= min_count) \
            .orderBy(col('count').desc())

    def print_topic_summary(self, df, top_n=10):
        """打印概要"""
        self.get_top_words(df, top_n=top_n).show()

    def get_top_tfidf_words(self, df, tokens_column='tokens', top_n=10):
        """兼容 UI 字段名: word, TF-IDF分数 (此处用 count 模拟)"""
        return self.get_top_words(df, tokens_column, top_n) \
                   .withColumnRenamed("count", "TF-IDF分数")

    def get_trending_words(self, df, top_n=10):
        """兼容 UI 字段名: word, recent_count, growth_rate"""
        return self.get_top_words(df, top_n=top_n) \
                   .withColumnRenamed("count", "recent_count") \
                   .withColumn("growth_rate", lit(0.0))
