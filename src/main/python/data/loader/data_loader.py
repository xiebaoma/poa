"""
数据加载器
从CSV/JSON文件或HDFS加载数据到Spark DataFrame
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pathlib import Path
import sys

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from utils.config_loader import load_config, get_data_paths


class DataLoader:
    """数据加载器类"""
    
    def __init__(self, spark_session=None):
        """
        初始化数据加载器
        
        Args:
            spark_session: SparkSession对象，如果为None则自动创建
        """
        if spark_session is None:
            config = load_config()
            spark_config = config.get('spark', {})
            
            self.spark = SparkSession.builder \
                .appName(spark_config.get('app_name', 'SentimentAnalysisSystem')) \
                .master(spark_config.get('master', 'local[*]')) \
                .config("spark.executor.memory", spark_config.get('executor_memory', '2g')) \
                .config("spark.driver.memory", spark_config.get('driver_memory', '1g')) \
                .getOrCreate()
        else:
            self.spark = spark_session
        
        # 定义数据Schema
        self.schema = StructType([
            StructField("doc_id", StringType(), nullable=False),
            StructField("content", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("source", StringType(), nullable=True)
        ])
    
    def load_from_csv(self, file_path, schema=None, header=True, infer_schema=False):
        """
        从CSV文件加载数据
        
        Args:
            file_path: CSV文件路径（支持本地路径或HDFS路径）
            schema: 数据Schema，如果为None则使用默认Schema或自动推断
            header: 是否包含表头
            infer_schema: 是否自动推断Schema（如果为True，则忽略schema参数）
            
        Returns:
            DataFrame: Spark DataFrame
        """
        file_path = str(file_path)
        
        if infer_schema:
            df = self.spark.read \
                .option("header", header) \
                .option("inferSchema", "true") \
                .option("encoding", "utf-8") \
                .csv(file_path)
        else:
            if schema is None:
                schema = self.schema
            
            df = self.spark.read \
                .option("header", header) \
                .schema(schema) \
                .option("encoding", "utf-8") \
                .csv(file_path)
        
        # 确保timestamp列是TimestampType
        if "timestamp" in df.columns:
            from pyspark.sql.functions import col
            df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
        
        return df
    
    def load_from_json(self, file_path, schema=None, multi_line=False):
        """
        从JSON文件加载数据
        
        Args:
            file_path: JSON文件路径（支持本地路径或HDFS路径）
            schema: 数据Schema，如果为None则自动推断
            multi_line: 是否是多行JSON格式
            
        Returns:
            DataFrame: Spark DataFrame
        """
        file_path = str(file_path)
        
        if schema is None:
            df = self.spark.read \
                .option("multiLine", multi_line) \
                .option("encoding", "utf-8") \
                .json(file_path)
        else:
            df = self.spark.read \
                .option("multiLine", multi_line) \
                .schema(schema) \
                .option("encoding", "utf-8") \
                .json(file_path)
        
        # 确保timestamp列是TimestampType
        if "timestamp" in df.columns:
            from pyspark.sql.functions import col
            df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
        
        return df
    
    def load_from_parquet(self, file_path):
        """
        从Parquet文件加载数据
        
        Args:
            file_path: Parquet文件路径（支持本地路径或HDFS路径）
            
        Returns:
            DataFrame: Spark DataFrame
        """
        file_path = str(file_path)
        return self.spark.read.parquet(file_path)
    
    def load_from_directory(self, directory_path, file_format='csv', **kwargs):
        """
        从目录加载多个文件
        
        Args:
            directory_path: 目录路径
            file_format: 文件格式（csv/json/parquet）
            **kwargs: 传递给具体加载方法的其他参数
            
        Returns:
            DataFrame: Spark DataFrame
        """
        directory_path = str(directory_path)
        
        if file_format.lower() == 'csv':
            return self.load_from_csv(directory_path, **kwargs)
        elif file_format.lower() == 'json':
            return self.load_from_json(directory_path, **kwargs)
        elif file_format.lower() == 'parquet':
            return self.load_from_parquet(directory_path)
        else:
            raise ValueError(f"不支持的文件格式: {file_format}")
    
    def load_raw_data(self, file_path=None, file_format='csv'):
        """
        从配置的原始数据路径加载数据
        
        Args:
            file_path: 文件路径，如果为None则从配置的raw_path加载所有文件
            file_format: 文件格式（csv/json/parquet）
            
        Returns:
            DataFrame: Spark DataFrame
        """
        if file_path is None:
            paths = get_data_paths()
            raw_path = paths['raw_path']
            
            # 查找指定格式的文件
            if file_format.lower() == 'csv':
                pattern = "*.csv"
            elif file_format.lower() == 'json':
                pattern = "*.json"
            elif file_format.lower() == 'parquet':
                pattern = "*.parquet"
            else:
                raise ValueError(f"不支持的文件格式: {file_format}")
            
            files = list(raw_path.glob(pattern))
            
            if not files:
                raise FileNotFoundError(f"在 {raw_path} 中未找到 {file_format} 文件")
            
            # 加载所有文件
            dfs = []
            for file in files:
                if file_format.lower() == 'csv':
                    df = self.load_from_csv(file)
                elif file_format.lower() == 'json':
                    df = self.load_from_json(file)
                elif file_format.lower() == 'parquet':
                    df = self.load_from_parquet(file)
                dfs.append(df)
            
            # 合并所有DataFrame
            from functools import reduce
            result_df = reduce(DataFrame.unionByName, dfs)
            return result_df
        else:
            return self.load_from_directory(file_path, file_format)
    
    def get_dataframe_info(self, df):
        """
        获取DataFrame的基本信息
        
        Args:
            df: Spark DataFrame
            
        Returns:
            dict: 包含行数、列数、Schema等信息
        """
        info = {
            'row_count': df.count(),
            'columns': df.columns,
            'schema': df.schema,
            'null_counts': {}
        }
        
        # 统计每列的空值数量
        for col_name in df.columns:
            null_count = df.filter(df[col_name].isNull()).count()
            info['null_counts'][col_name] = null_count
        
        return info
    
    def print_dataframe_info(self, df, show_sample=True, sample_size=10):
        """
        打印DataFrame的基本信息和样本数据
        
        Args:
            df: Spark DataFrame
            show_sample: 是否显示样本数据
            sample_size: 样本数据行数
        """
        info = self.get_dataframe_info(df)
        
        print("=" * 60)
        print("DataFrame 信息")
        print("=" * 60)
        print(f"行数: {info['row_count']}")
        print(f"列数: {len(info['columns'])}")
        print(f"列名: {', '.join(info['columns'])}")
        print("\n空值统计:")
        for col_name, null_count in info['null_counts'].items():
            print(f"  {col_name}: {null_count} ({null_count/info['row_count']*100:.2f}%)" if info['row_count'] > 0 else f"  {col_name}: {null_count}")
        
        if show_sample:
            print(f"\n样本数据 (前 {sample_size} 行):")
            print("-" * 60)
            df.show(sample_size, truncate=False)
        
        print("=" * 60)


def main():
    """主函数，用于测试数据加载器"""
    loader = DataLoader()
    
    # 从配置的原始数据路径加载数据
    try:
        print("尝试从配置路径加载数据...")
        df = loader.load_raw_data(file_format='csv')
        loader.print_dataframe_info(df)
    except FileNotFoundError as e:
        print(f"未找到数据文件: {e}")
        print("请先运行数据生成器生成测试数据")


if __name__ == "__main__":
    main()

