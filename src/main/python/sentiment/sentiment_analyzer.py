"""
情感分析器 - 重构精简版
解决 Windows 环境下 UDF 频繁崩溃的问题
"""
from pathlib import Path
import os
import sys
import platform
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, FloatType, IntegerType, StructType, StructField

# ==========================================
# Windows 极致稳定性补丁
# ==========================================
if platform.system() == 'Windows':
    py_exe = sys.executable.replace('\\', '/')
    os.environ['PYSPARK_PYTHON'] = py_exe
    os.environ['PYSPARK_DRIVER_PYTHON'] = py_exe
    os.environ['PYSPARK_NO_DAEMON'] = '1'
    os.environ['PYTHONUNBUFFERED'] = '1'

class SentimentAnalyzer:
    """精简版情感分析器"""
    
    def __init__(self, spark_session=None, positive_words_path=None, negative_words_path=None):
        self.spark = spark_session
        self.pos_set = self._load_dict(positive_words_path or 
            Path(__file__).parent.parent.parent / "resources/dict/positive_words.txt")
        self.neg_set = self._load_dict(negative_words_path or 
            Path(__file__).parent.parent.parent / "resources/dict/negative_words.txt")

    def _load_dict(self, path):
        path = Path(path)
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                words = set(line.strip() for line in f if line.strip())
            print(f"已加载 {len(words)} 个词点: {path.name}")
            return words
        return set()

    def _get_analyze_func(self):
        """合并所有逻辑到一个 UDF，减少进程切换开销"""
        pos_set = self.pos_set
        neg_set = self.neg_set
        
        def analyze(tokens):
            if not tokens:
                return ("neutral", 0.0, 0, 0)
            
            p_count = sum(1 for t in tokens if t in pos_set)
            n_count = sum(1 for t in tokens if t in neg_set)
            
            total = len(tokens)
            score = (p_count - n_count) / total if total > 0 else 0.0
            
            if p_count > n_count:
                label = "positive"
            elif n_count > p_count:
                label = "negative"
            else:
                label = "neutral"
                
            return (label, float(score), p_count, n_count)
        return analyze

    def analyze(self, df, tokens_column='tokens', **kwargs):
        """执行分析"""
        schema = StructType([
            StructField("sentiment_label", StringType(), False),
            StructField("sentiment_score", FloatType(), False),
            StructField("positive_count", IntegerType(), False),
            StructField("negative_count", IntegerType(), False)
        ])
        
        analyze_udf = udf(self._get_analyze_func(), schema)
        
        print(f"执行情感分析 (UDF 合并版)...")
        df = df.withColumn("_res", analyze_udf(col(tokens_column)))
        
        return df.withColumn("sentiment_label", col("_res.sentiment_label")) \
                 .withColumn("sentiment_score", col("_res.sentiment_score")) \
                 .withColumn("positive_count", col("_res.positive_count")) \
                 .withColumn("negative_count", col("_res.negative_count")) \
                 .drop("_res")

    def print_statistics(self, df):
        """简单统计"""
        print(f"情感分析完成，总计: {df.count()} 条记录")
        df.groupBy("sentiment_label").count().show()
