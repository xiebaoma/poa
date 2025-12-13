"""
热点话题挖掘器
实现词频统计、TF-IDF、Top-N关键词提取和话题趋势分析
"""
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, explode, count, sum as spark_sum, desc, asc,
    date_format, window, collect_list, size, lit,
    row_number, dense_rank, log, when
)
from pyspark.sql.window import Window
from pyspark.ml.feature import HashingTF, IDF, CountVectorizer
from pyspark.ml import Pipeline


class TopicMiner:
    """热点话题挖掘器类"""
    
    def __init__(self, spark_session=None, config=None):
        """
        初始化热点话题挖掘器
        
        Args:
            spark_session: SparkSession对象
            config: 配置字典
        """
        if config is None:
            from utils.config_loader import load_config
            config = load_config()
        
        self.config = config
        self.spark = spark_session
        
        # 获取配置参数
        topic_config = config.get('processing', {}).get('topic', {})
        self.default_top_n = topic_config.get('top_n', 20)
    
    def word_count(self, df, tokens_column='tokens'):
        """
        词频统计
        
        Args:
            df: 包含分词结果的DataFrame
            tokens_column: 分词列名
            
        Returns:
            DataFrame: 词频统计结果，包含word和count列
        """
        return df.select(explode(col(tokens_column)).alias('word')) \
            .groupBy('word') \
            .agg(count('*').alias('count')) \
            .orderBy(col('count').desc())
    
    def get_top_words(self, df, tokens_column='tokens', top_n=None):
        """
        获取Top-N高频词
        
        Args:
            df: 包含分词结果的DataFrame
            tokens_column: 分词列名
            top_n: 返回前N个词，默认使用配置值
            
        Returns:
            DataFrame: Top-N高频词
        """
        if top_n is None:
            top_n = self.default_top_n
        
        return self.word_count(df, tokens_column).limit(top_n)
    
    def word_count_by_source(self, df, tokens_column='tokens', source_column='source'):
        """
        按数据源统计词频
        
        Args:
            df: DataFrame
            tokens_column: 分词列名
            source_column: 数据源列名
            
        Returns:
            DataFrame: 按数据源的词频统计
        """
        return df.select(col(source_column), explode(col(tokens_column)).alias('word')) \
            .groupBy(source_column, 'word') \
            .agg(count('*').alias('count')) \
            .orderBy(source_column, col('count').desc())
    
    def get_top_words_by_source(self, df, tokens_column='tokens', source_column='source', top_n=None):
        """
        按数据源获取Top-N高频词
        
        Args:
            df: DataFrame
            tokens_column: 分词列名
            source_column: 数据源列名
            top_n: 每个数据源返回前N个词
            
        Returns:
            DataFrame: 按数据源的Top-N高频词
        """
        if top_n is None:
            top_n = self.default_top_n
        
        word_counts = self.word_count_by_source(df, tokens_column, source_column)
        
        # 使用窗口函数获取每个source的top-n
        window_spec = Window.partitionBy(source_column).orderBy(col('count').desc())
        
        return word_counts.withColumn('rank', row_number().over(window_spec)) \
            .filter(col('rank') <= top_n) \
            .drop('rank')
    
    def calculate_tfidf(self, df, tokens_column='tokens', num_features=1000):
        """
        计算TF-IDF
        
        Args:
            df: 包含分词结果的DataFrame
            tokens_column: 分词列名
            num_features: 特征数量
            
        Returns:
            tuple: (处理后的DataFrame, TF-IDF模型)
        """
        # 使用HashingTF计算词频
        hashing_tf = HashingTF(inputCol=tokens_column, outputCol="tf_features", numFeatures=num_features)
        
        # 计算IDF
        idf = IDF(inputCol="tf_features", outputCol="tfidf_features")
        
        # 构建Pipeline
        pipeline = Pipeline(stages=[hashing_tf, idf])
        
        # 拟合模型
        model = pipeline.fit(df)
        
        # 转换数据
        df_tfidf = model.transform(df)
        
        return df_tfidf, model
    
    def calculate_tfidf_simple(self, df, tokens_column='tokens'):
        """
        简化版TF-IDF计算（基于DataFrame操作）
        
        Args:
            df: 包含分词结果的DataFrame
            tokens_column: 分词列名
            
        Returns:
            DataFrame: 包含word, tf, idf, tfidf的DataFrame
        """
        # 总文档数
        total_docs = df.count()
        
        # 展开词汇并计算词频（TF）
        words_df = df.select(
            col('doc_id'),
            explode(col(tokens_column)).alias('word')
        )
        
        # 每个文档的词频
        tf_df = words_df.groupBy('doc_id', 'word') \
            .agg(count('*').alias('term_count'))
        
        # 每个文档的总词数
        doc_word_count = words_df.groupBy('doc_id') \
            .agg(count('*').alias('doc_total'))
        
        # 计算TF
        tf_df = tf_df.join(doc_word_count, 'doc_id') \
            .withColumn('tf', col('term_count') / col('doc_total'))
        
        # 计算IDF：log(总文档数 / 包含该词的文档数)
        doc_freq = words_df.groupBy('word') \
            .agg(count('doc_id').alias('doc_freq'))
        
        idf_df = doc_freq.withColumn('idf', log(lit(total_docs) / col('doc_freq')))
        
        # 计算TF-IDF
        tfidf_df = tf_df.join(idf_df, 'word') \
            .withColumn('tfidf', col('tf') * col('idf')) \
            .select('doc_id', 'word', 'tf', 'idf', 'tfidf')
        
        return tfidf_df
    
    def get_top_tfidf_words(self, df, tokens_column='tokens', top_n=None):
        """
        获取TF-IDF值最高的词
        
        Args:
            df: DataFrame
            tokens_column: 分词列名
            top_n: 返回前N个词
            
        Returns:
            DataFrame: Top-N TF-IDF词
        """
        if top_n is None:
            top_n = self.default_top_n
        
        tfidf_df = self.calculate_tfidf_simple(df, tokens_column)
        
        # 计算每个词的平均TF-IDF
        avg_tfidf = tfidf_df.groupBy('word') \
            .agg(spark_sum('tfidf').alias('total_tfidf')) \
            .orderBy(col('total_tfidf').desc())
        
        return avg_tfidf.limit(top_n)
    
    def word_count_by_time(self, df, tokens_column='tokens', time_column='timestamp', 
                           time_format='yyyy-MM-dd'):
        """
        按时间统计词频
        
        Args:
            df: DataFrame
            tokens_column: 分词列名
            time_column: 时间列名
            time_format: 时间格式（yyyy-MM-dd, yyyy-MM-dd HH, yyyy-MM等）
            
        Returns:
            DataFrame: 按时间的词频统计
        """
        return df.withColumn('time_bucket', date_format(col(time_column), time_format)) \
            .select('time_bucket', explode(col(tokens_column)).alias('word')) \
            .groupBy('time_bucket', 'word') \
            .agg(count('*').alias('count')) \
            .orderBy('time_bucket', col('count').desc())
    
    def get_trending_words(self, df, tokens_column='tokens', time_column='timestamp',
                          time_format='yyyy-MM-dd', top_n=None):
        """
        获取每个时间段的热点词
        
        Args:
            df: DataFrame
            tokens_column: 分词列名
            time_column: 时间列名
            time_format: 时间格式
            top_n: 每个时间段返回前N个词
            
        Returns:
            DataFrame: 每个时间段的Top-N热点词
        """
        if top_n is None:
            top_n = self.default_top_n
        
        word_counts = self.word_count_by_time(df, tokens_column, time_column, time_format)
        
        # 使用窗口函数获取每个时间段的top-n
        window_spec = Window.partitionBy('time_bucket').orderBy(col('count').desc())
        
        return word_counts.withColumn('rank', row_number().over(window_spec)) \
            .filter(col('rank') <= top_n) \
            .drop('rank')
    
    def get_word_trend(self, df, words, tokens_column='tokens', time_column='timestamp',
                       time_format='yyyy-MM-dd'):
        """
        获取指定词的趋势变化
        
        Args:
            df: DataFrame
            words: 要追踪的词列表
            tokens_column: 分词列名
            time_column: 时间列名
            time_format: 时间格式
            
        Returns:
            DataFrame: 指定词的时间趋势
        """
        word_counts = self.word_count_by_time(df, tokens_column, time_column, time_format)
        
        return word_counts.filter(col('word').isin(words)) \
            .orderBy('word', 'time_bucket')
    
    def get_cooccurrence(self, df, tokens_column='tokens', min_count=2):
        """
        计算词共现关系
        
        Args:
            df: DataFrame
            tokens_column: 分词列名
            min_count: 最小共现次数
            
        Returns:
            DataFrame: 词共现统计
        """
        from pyspark.sql.functions import arrays_zip, flatten, array
        from itertools import combinations
        
        # 生成词对的UDF
        def generate_pairs(tokens):
            if tokens is None or len(tokens) < 2:
                return []
            pairs = []
            unique_tokens = list(set(tokens))
            for i in range(len(unique_tokens)):
                for j in range(i + 1, len(unique_tokens)):
                    pair = tuple(sorted([unique_tokens[i], unique_tokens[j]]))
                    pairs.append(f"{pair[0]}|{pair[1]}")
            return pairs
        
        from pyspark.sql.types import ArrayType, StringType
        from pyspark.sql.functions import udf
        
        pairs_udf = udf(generate_pairs, ArrayType(StringType()))
        
        # 生成词对并统计
        cooccurrence = df.withColumn('pairs', pairs_udf(col(tokens_column))) \
            .select(explode(col('pairs')).alias('pair')) \
            .groupBy('pair') \
            .agg(count('*').alias('count')) \
            .filter(col('count') >= min_count) \
            .orderBy(col('count').desc())
        
        # 分割词对
        from pyspark.sql.functions import split
        cooccurrence = cooccurrence \
            .withColumn('word1', split(col('pair'), '\\|').getItem(0)) \
            .withColumn('word2', split(col('pair'), '\\|').getItem(1)) \
            .select('word1', 'word2', 'count')
        
        return cooccurrence
    
    def get_topic_summary(self, df, tokens_column='tokens', time_column='timestamp',
                          time_format='yyyy-MM-dd', top_n=10):
        """
        生成话题摘要报告
        
        Args:
            df: DataFrame
            tokens_column: 分词列名
            time_column: 时间列名
            time_format: 时间格式
            top_n: 每个维度返回前N个词
            
        Returns:
            dict: 话题摘要
        """
        summary = {}
        
        # 全局热点词
        top_words = self.get_top_words(df, tokens_column, top_n).collect()
        summary['top_words'] = [(row['word'], row['count']) for row in top_words]
        
        # TF-IDF关键词
        tfidf_words = self.get_top_tfidf_words(df, tokens_column, top_n).collect()
        summary['tfidf_keywords'] = [(row['word'], float(row['total_tfidf'])) for row in tfidf_words]
        
        # 按时间的热点词
        trending = self.get_trending_words(df, tokens_column, time_column, time_format, top_n).collect()
        time_topics = {}
        for row in trending:
            time_bucket = row['time_bucket']
            if time_bucket not in time_topics:
                time_topics[time_bucket] = []
            time_topics[time_bucket].append((row['word'], row['count']))
        summary['trending_by_time'] = time_topics
        
        return summary
    
    def print_topic_summary(self, df, tokens_column='tokens', time_column='timestamp',
                            time_format='yyyy-MM-dd', top_n=10):
        """打印话题摘要"""
        summary = self.get_topic_summary(df, tokens_column, time_column, time_format, top_n)
        
        print("=" * 60)
        print("热点话题摘要")
        print("=" * 60)
        
        print(f"\n【全局热点词 Top-{top_n}】")
        for word, count in summary['top_words']:
            print(f"  {word}: {count}")
        
        print(f"\n【TF-IDF关键词 Top-{top_n}】")
        for word, score in summary['tfidf_keywords']:
            print(f"  {word}: {score:.4f}")
        
        print(f"\n【按时间的热点词】")
        for time_bucket in sorted(summary['trending_by_time'].keys()):
            words = summary['trending_by_time'][time_bucket][:5]
            word_str = ', '.join([w[0] for w in words])
            print(f"  {time_bucket}: {word_str}")
        
        print("=" * 60)

