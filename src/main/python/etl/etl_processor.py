"""
ETL处理器
整合文本清洗、分词、停用词过滤等功能的完整ETL流水线
"""
from pathlib import Path
import sys

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, monotonically_increasing_id

from .text_cleaner import TextCleaner
from .tokenizer import Tokenizer


class ETLProcessor:
    """ETL处理器类"""
    
    def __init__(self, spark_session=None, config=None):
        """
        初始化ETL处理器
        
        Args:
            spark_session: SparkSession对象，如果为None则自动创建
            config: 配置字典
        """
        # 加载配置
        if config is None:
            from utils.config_loader import load_config
            config = load_config()
        
        self.config = config
        
        # 创建SparkSession
        if spark_session is None:
            spark_config = config.get('spark', {})
            self.spark = SparkSession.builder \
                .appName(spark_config.get('app_name', 'SentimentAnalysisSystem')) \
                .master(spark_config.get('master', 'local[*]')) \
                .config("spark.executor.memory", spark_config.get('executor_memory', '2g')) \
                .config("spark.driver.memory", spark_config.get('driver_memory', '1g')) \
                .getOrCreate()
        else:
            self.spark = spark_session
        
        # 初始化清洗器和分词器
        self.cleaner = TextCleaner()
        
        # 获取停用词表路径
        dict_config = config.get('dictionary', {})
        stopwords_path = dict_config.get('stopwords')
        
        if stopwords_path:
            # 相对路径转绝对路径
            project_root = Path(__file__).parent.parent.parent.parent
            stopwords_path = project_root / stopwords_path
        
        self.tokenizer = Tokenizer(self.spark, stopwords_path)
        
        # 广播停用词表
        self.tokenizer.broadcast_stopwords(self.spark)
    
    def process(self, df, text_column='content', 
                clean_text=True, deduplicate=True, 
                tokenize=True, filter_stopwords=True,
                min_text_length=2, min_tokens=1):
        """
        执行完整的ETL处理流水线
        
        Args:
            df: 输入的Spark DataFrame
            text_column: 文本列名
            clean_text: 是否进行文本清洗
            deduplicate: 是否去重
            tokenize: 是否分词
            filter_stopwords: 是否过滤停用词
            min_text_length: 最小文本长度
            min_tokens: 最少分词数量
            
        Returns:
            DataFrame: 处理后的DataFrame
        """
        print("=" * 60)
        print("开始ETL处理")
        print("=" * 60)
        
        original_count = df.count()
        print(f"原始数据量: {original_count}")
        
        # 缓存原始数据
        df = df.cache()
        
        # 步骤1：文本清洗
        if clean_text:
            print("\n【步骤1】文本清洗...")
            df = self.cleaner.clean_pipeline(
                df, 
                text_column=text_column,
                deduplicate=deduplicate,
                remove_empty=True,
                min_length=min_text_length
            )
        
        # 步骤2：分词
        if tokenize:
            print("\n【步骤2】分词处理...")
            if filter_stopwords:
                df = self.tokenizer.tokenize_and_filter(df, text_column, 'tokens')
            else:
                df = self.tokenizer.tokenize(df, text_column, 'tokens')
            
            # 去除空分词结果
            df = self.tokenizer.remove_empty_tokens(df, 'tokens', min_tokens)
            
            token_count = df.count()
            print(f"分词后数据量: {token_count}")
            
            # 添加分词字符串列（便于查看）
            df = self.tokenizer.tokens_to_string(df, 'tokens', 'tokens_str')
        
        # 添加处理后的行ID
        df = df.withColumn("processed_id", monotonically_increasing_id())
        
        final_count = df.count()
        print(f"\n处理完成！最终数据量: {final_count}")
        print(f"数据保留率: {final_count/original_count*100:.2f}%")
        print("=" * 60)
        
        return df
    
    def process_and_save(self, df, output_path=None, output_format='parquet',
                        **process_kwargs):
        """
        执行ETL处理并保存结果
        
        Args:
            df: 输入的Spark DataFrame
            output_path: 输出路径，如果为None则使用配置的processed_path
            output_format: 输出格式（parquet/csv/json）
            **process_kwargs: 传递给process方法的其他参数
            
        Returns:
            DataFrame: 处理后的DataFrame
        """
        # 执行ETL处理
        df_processed = self.process(df, **process_kwargs)
        
        # 确定输出路径
        if output_path is None:
            from utils.config_loader import get_data_paths
            paths = get_data_paths(self.config)
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = paths['processed_path'] / f"processed_data_{timestamp}"
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存数据
        print(f"\n保存数据到: {output_path}")
        
        if output_format.lower() == 'parquet':
            df_processed.write.mode('overwrite').parquet(str(output_path))
        elif output_format.lower() == 'csv':
            # CSV不支持数组类型，需要转换
            df_to_save = df_processed.drop('tokens')
            df_to_save.write.mode('overwrite').option('header', 'true').csv(str(output_path))
        elif output_format.lower() == 'json':
            df_processed.write.mode('overwrite').json(str(output_path))
        else:
            raise ValueError(f"不支持的输出格式: {output_format}")
        
        print(f"数据保存成功！格式: {output_format}")
        
        return df_processed
    
    def get_statistics(self, df, tokens_column='tokens'):
        """
        获取处理后数据的统计信息
        
        Args:
            df: 处理后的Spark DataFrame
            tokens_column: 分词列名
            
        Returns:
            dict: 统计信息字典
        """
        from pyspark.sql.functions import avg, min, max, size, count
        
        stats = {}
        
        # 基本统计
        stats['total_records'] = df.count()
        
        # 数据源分布
        if 'source' in df.columns:
            source_dist = df.groupBy('source').count().collect()
            stats['source_distribution'] = {row['source']: row['count'] for row in source_dist}
        
        # 分词统计
        if tokens_column in df.columns:
            token_stats = df.agg(
                avg(size(col(tokens_column))).alias('avg_tokens'),
                min(size(col(tokens_column))).alias('min_tokens'),
                max(size(col(tokens_column))).alias('max_tokens')
            ).collect()[0]
            
            stats['token_stats'] = {
                'avg_tokens': float(token_stats['avg_tokens']) if token_stats['avg_tokens'] else 0,
                'min_tokens': int(token_stats['min_tokens']) if token_stats['min_tokens'] else 0,
                'max_tokens': int(token_stats['max_tokens']) if token_stats['max_tokens'] else 0
            }
        
        return stats
    
    def print_statistics(self, df, tokens_column='tokens'):
        """
        打印处理后数据的统计信息
        
        Args:
            df: 处理后的Spark DataFrame
            tokens_column: 分词列名
        """
        stats = self.get_statistics(df, tokens_column)
        
        print("\n" + "=" * 60)
        print("数据统计信息")
        print("=" * 60)
        print(f"总记录数: {stats['total_records']}")
        
        if 'source_distribution' in stats:
            print("\n数据源分布:")
            for source, count in stats['source_distribution'].items():
                print(f"  {source}: {count} ({count/stats['total_records']*100:.1f}%)")
        
        if 'token_stats' in stats:
            print("\n分词统计:")
            print(f"  平均分词数: {stats['token_stats']['avg_tokens']:.2f}")
            print(f"  最小分词数: {stats['token_stats']['min_tokens']}")
            print(f"  最大分词数: {stats['token_stats']['max_tokens']}")
        
        print("=" * 60)
    
    def get_top_words(self, df, tokens_column='tokens', top_n=20):
        """
        获取高频词
        
        Args:
            df: 处理后的Spark DataFrame
            tokens_column: 分词列名
            top_n: 返回前N个高频词
            
        Returns:
            DataFrame: 高频词DataFrame
        """
        word_count = self.tokenizer.get_word_count(df, tokens_column)
        return word_count.limit(top_n)


def main():
    """测试ETL处理器"""
    import sys
    from pathlib import Path
    
    # 添加项目路径
    project_root = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(project_root / "src/main/python"))
    
    from data.generator import DataGenerator
    from data.loader import DataLoader
    
    # 生成测试数据
    print("生成测试数据...")
    generator = DataGenerator()
    generator.generate_dataset(
        num_records=200,
        start_date="2025-01-01",
        end_date="2025-01-15"
    )
    
    # 加载数据
    print("\n加载数据...")
    loader = DataLoader()
    df = loader.load_raw_data(file_format='csv')
    
    # 显示原始数据
    print("\n原始数据预览:")
    df.show(5, truncate=False)
    
    # ETL处理
    processor = ETLProcessor(spark_session=loader.spark)
    df_processed = processor.process(df)
    
    # 显示处理后的数据
    print("\n处理后数据预览:")
    df_processed.select('doc_id', 'content', 'tokens_str', 'source').show(5, truncate=False)
    
    # 显示统计信息
    processor.print_statistics(df_processed)
    
    # 显示高频词
    print("\n高频词 Top-10:")
    top_words = processor.get_top_words(df_processed, top_n=10)
    top_words.show()


if __name__ == "__main__":
    main()

