"""
情感分析器
基于情感词典的文本情感分析
"""
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, when, lit, array_contains, size
from pyspark.sql.types import StringType, FloatType, IntegerType, StructType, StructField


class SentimentAnalyzer:
    """情感分析器类"""
    
    # 情感标签常量
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    
    def __init__(self, spark_session=None, positive_words_path=None, negative_words_path=None):
        """
        初始化情感分析器
        
        Args:
            spark_session: SparkSession对象
            positive_words_path: 正面情感词表路径
            negative_words_path: 负面情感词表路径
        """
        self.spark = spark_session
        self.positive_words = set()
        self.negative_words = set()
        self.positive_broadcast = None
        self.negative_broadcast = None
        
        # 加载情感词典
        if positive_words_path is None:
            positive_words_path = Path(__file__).parent.parent.parent / "resources/dict/positive_words.txt"
        if negative_words_path is None:
            negative_words_path = Path(__file__).parent.parent.parent / "resources/dict/negative_words.txt"
        
        self._load_sentiment_dict(positive_words_path, negative_words_path)
        self._register_udfs()
    
    def _load_sentiment_dict(self, positive_path, negative_path):
        """加载情感词典"""
        positive_path = Path(positive_path)
        negative_path = Path(negative_path)
        
        if positive_path.exists():
            with open(positive_path, 'r', encoding='utf-8') as f:
                self.positive_words = set(line.strip() for line in f if line.strip())
            print(f"已加载 {len(self.positive_words)} 个正面情感词")
        
        if negative_path.exists():
            with open(negative_path, 'r', encoding='utf-8') as f:
                self.negative_words = set(line.strip() for line in f if line.strip())
            print(f"已加载 {len(self.negative_words)} 个负面情感词")
    
    def broadcast_dict(self, spark_session):
        """广播情感词典到集群"""
        self.spark = spark_session
        self.positive_broadcast = spark_session.sparkContext.broadcast(self.positive_words)
        self.negative_broadcast = spark_session.sparkContext.broadcast(self.negative_words)
        print("情感词典已广播到集群")
    
    def _register_udfs(self):
        """注册Spark UDF"""
        positive_words = self.positive_words
        negative_words = self.negative_words
        
        # 计算正面词数量
        def count_positive(tokens):
            if tokens is None:
                return 0
            return sum(1 for t in tokens if t in positive_words)
        
        # 计算负面词数量
        def count_negative(tokens):
            if tokens is None:
                return 0
            return sum(1 for t in tokens if t in negative_words)
        
        # 计算情感得分
        def calc_sentiment_score(tokens):
            if tokens is None or len(tokens) == 0:
                return 0.0
            pos_count = sum(1 for t in tokens if t in positive_words)
            neg_count = sum(1 for t in tokens if t in negative_words)
            total = len(tokens)
            return (pos_count - neg_count) / total
        
        # 判断情感标签
        def get_sentiment_label(tokens):
            if tokens is None or len(tokens) == 0:
                return "neutral"
            pos_count = sum(1 for t in tokens if t in positive_words)
            neg_count = sum(1 for t in tokens if t in negative_words)
            if pos_count > neg_count:
                return "positive"
            elif neg_count > pos_count:
                return "negative"
            else:
                return "neutral"
        
        self.count_positive_udf = udf(count_positive, IntegerType())
        self.count_negative_udf = udf(count_negative, IntegerType())
        self.sentiment_score_udf = udf(calc_sentiment_score, FloatType())
        self.sentiment_label_udf = udf(get_sentiment_label, StringType())
    
    def analyze(self, df, tokens_column='tokens', include_counts=True):
        """
        对DataFrame进行情感分析
        
        Args:
            df: 包含分词结果的Spark DataFrame
            tokens_column: 分词列名
            include_counts: 是否包含正负面词计数
            
        Returns:
            DataFrame: 包含情感分析结果的DataFrame
        """
        # 计算情感得分和标签
        df = df.withColumn("sentiment_score", self.sentiment_score_udf(col(tokens_column)))
        df = df.withColumn("sentiment_label", self.sentiment_label_udf(col(tokens_column)))
        
        # 可选：包含正负面词计数
        if include_counts:
            df = df.withColumn("positive_count", self.count_positive_udf(col(tokens_column)))
            df = df.withColumn("negative_count", self.count_negative_udf(col(tokens_column)))
        
        return df
    
    def analyze_text(self, df, text_column='content'):
        """
        直接对原始文本进行情感分析（不需要预先分词）
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            
        Returns:
            DataFrame: 包含情感分析结果的DataFrame
        """
        positive_words = self.positive_words
        negative_words = self.negative_words
        
        def analyze_text_func(text):
            if text is None or len(text) == 0:
                return ("neutral", 0.0, 0, 0)
            
            pos_count = sum(1 for word in positive_words if word in text)
            neg_count = sum(1 for word in negative_words if word in text)
            
            if pos_count > neg_count:
                label = "positive"
            elif neg_count > pos_count:
                label = "negative"
            else:
                label = "neutral"
            
            score = (pos_count - neg_count) / max(pos_count + neg_count, 1)
            
            return (label, float(score), pos_count, neg_count)
        
        schema = StructType([
            StructField("sentiment_label", StringType(), False),
            StructField("sentiment_score", FloatType(), False),
            StructField("positive_count", IntegerType(), False),
            StructField("negative_count", IntegerType(), False)
        ])
        
        analyze_udf = udf(analyze_text_func, schema)
        
        df = df.withColumn("_sentiment_result", analyze_udf(col(text_column)))
        df = df.withColumn("sentiment_label", col("_sentiment_result.sentiment_label"))
        df = df.withColumn("sentiment_score", col("_sentiment_result.sentiment_score"))
        df = df.withColumn("positive_count", col("_sentiment_result.positive_count"))
        df = df.withColumn("negative_count", col("_sentiment_result.negative_count"))
        df = df.drop("_sentiment_result")
        
        return df
    
    def get_sentiment_distribution(self, df, label_column='sentiment_label'):
        """
        获取情感分布统计
        
        Args:
            df: 包含情感标签的DataFrame
            label_column: 情感标签列名
            
        Returns:
            DataFrame: 情感分布统计
        """
        from pyspark.sql.functions import count, round as spark_round
        
        total = df.count()
        
        distribution = df.groupBy(label_column) \
            .agg(count("*").alias("count")) \
            .withColumn("percentage", spark_round(col("count") / total * 100, 2)) \
            .orderBy(col("count").desc())
        
        return distribution
    
    def get_sentiment_by_source(self, df, source_column='source', label_column='sentiment_label'):
        """
        按数据源统计情感分布
        
        Args:
            df: 包含情感标签的DataFrame
            source_column: 数据源列名
            label_column: 情感标签列名
            
        Returns:
            DataFrame: 按数据源的情感分布
        """
        from pyspark.sql.functions import count
        
        return df.groupBy(source_column, label_column) \
            .agg(count("*").alias("count")) \
            .orderBy(source_column, col("count").desc())
    
    def get_extreme_samples(self, df, score_column='sentiment_score', n=5):
        """
        获取极端情感样本
        
        Args:
            df: 包含情感得分的DataFrame
            score_column: 情感得分列名
            n: 每类返回的样本数
            
        Returns:
            tuple: (最正面样本DataFrame, 最负面样本DataFrame)
        """
        most_positive = df.orderBy(col(score_column).desc()).limit(n)
        most_negative = df.orderBy(col(score_column).asc()).limit(n)
        
        return most_positive, most_negative
    
    def filter_by_sentiment(self, df, sentiment, label_column='sentiment_label'):
        """
        按情感标签过滤数据
        
        Args:
            df: DataFrame
            sentiment: 情感类型（positive/negative/neutral）
            label_column: 情感标签列名
            
        Returns:
            DataFrame: 过滤后的DataFrame
        """
        return df.filter(col(label_column) == sentiment)
    
    def get_statistics(self, df):
        """
        获取情感分析统计信息
        
        Args:
            df: 包含情感分析结果的DataFrame
            
        Returns:
            dict: 统计信息
        """
        from pyspark.sql.functions import avg, min, max, stddev
        
        total = df.count()
        
        # 情感分布
        distribution = df.groupBy("sentiment_label").count().collect()
        sentiment_dist = {row["sentiment_label"]: row["count"] for row in distribution}
        
        # 情感得分统计
        score_stats = df.agg(
            avg("sentiment_score").alias("avg_score"),
            min("sentiment_score").alias("min_score"),
            max("sentiment_score").alias("max_score"),
            stddev("sentiment_score").alias("std_score")
        ).collect()[0]
        
        return {
            "total_records": total,
            "sentiment_distribution": sentiment_dist,
            "positive_ratio": sentiment_dist.get("positive", 0) / total * 100 if total > 0 else 0,
            "negative_ratio": sentiment_dist.get("negative", 0) / total * 100 if total > 0 else 0,
            "neutral_ratio": sentiment_dist.get("neutral", 0) / total * 100 if total > 0 else 0,
            "avg_score": float(score_stats["avg_score"]) if score_stats["avg_score"] else 0,
            "min_score": float(score_stats["min_score"]) if score_stats["min_score"] else 0,
            "max_score": float(score_stats["max_score"]) if score_stats["max_score"] else 0,
            "std_score": float(score_stats["std_score"]) if score_stats["std_score"] else 0
        }
    
    def print_statistics(self, df):
        """打印情感分析统计信息"""
        stats = self.get_statistics(df)
        
        print("=" * 60)
        print("情感分析统计")
        print("=" * 60)
        print(f"总记录数: {stats['total_records']}")
        print(f"\n情感分布:")
        print(f"  正面: {stats['sentiment_distribution'].get('positive', 0)} ({stats['positive_ratio']:.1f}%)")
        print(f"  负面: {stats['sentiment_distribution'].get('negative', 0)} ({stats['negative_ratio']:.1f}%)")
        print(f"  中性: {stats['sentiment_distribution'].get('neutral', 0)} ({stats['neutral_ratio']:.1f}%)")
        print(f"\n情感得分统计:")
        print(f"  平均: {stats['avg_score']:.4f}")
        print(f"  最小: {stats['min_score']:.4f}")
        print(f"  最大: {stats['max_score']:.4f}")
        print(f"  标准差: {stats['std_score']:.4f}")
        print("=" * 60)

