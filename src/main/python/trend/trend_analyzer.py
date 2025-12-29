"""
舆情趋势分析器 - 精简版
主要使用原生 Spark SQL 算子，稳定性极高
"""
from pathlib import Path
import os
import sys
import platform
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, date_format, lit, when, round as spark_round

# ==========================================
# Windows 极致稳定性补丁
# ==========================================
if platform.system() == 'Windows':
    py_exe = sys.executable.replace('\\', '/')
    os.environ['PYSPARK_PYTHON'] = py_exe
    os.environ['PYSPARK_DRIVER_PYTHON'] = py_exe
    os.environ['PYSPARK_NO_DAEMON'] = '1'
    os.environ['PYTHONUNBUFFERED'] = '1'

class TrendAnalyzer:
    """精简版趋势分析器"""
    
    def __init__(self, spark_session=None, config=None):
        self.spark = spark_session
        self.config = config or {}

    def sentiment_by_time(self, df, time_column='timestamp', label_column='sentiment_label', time_window='day'):
        """按时间统计情感分布 (兼容 UI 字段名)"""
        time_format = 'yyyy-MM-dd' if time_window == 'day' else 'yyyy-MM-dd HH'
        
        print(f"执行趋势分析 (粒度: {time_window})...")
        
        # 统计
        res = df.withColumn('time_bucket', date_format(col(time_column), time_format)) \
            .groupBy('time_bucket', label_column) \
            .agg(count('*').alias('count'))
        
        # 透视
        pivoted = res.groupBy('time_bucket').pivot(label_column).sum('count').fillna(0)
        
        # 补充缺失列并重命名为 UI 期望的 _count 格式
        sentiments = ['positive', 'negative', 'neutral']
        for s in sentiments:
            if s not in pivoted.columns:
                pivoted = pivoted.withColumn(s, lit(0))
            pivoted = pivoted.withColumnRenamed(s, f"{s}_count")
            
        pivoted = pivoted.withColumn('total_count', col('positive_count') + col('negative_count') + col('neutral_count'))
        
        return pivoted.withColumn('negative_ratio', col('negative_count') / col('total_count')) \
                      .orderBy('time_bucket')

    def detect_negative_surge(self, df, **kwargs):
        """检测负面激增 (简单过滤)"""
        trend_df = self.sentiment_by_time(df, **kwargs)
        return trend_df.filter(col('negative_ratio') > 0.4)

    def print_trend_summary(self, df, **kwargs):
        """打印趋势摘要"""
        self.sentiment_by_time(df, **kwargs).show()
