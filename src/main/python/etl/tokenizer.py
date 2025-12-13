"""
分词器
用于中文文本分词处理
"""
import re
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, col, explode, split, array_remove, size
from pyspark.sql.types import StringType, ArrayType
from pyspark.broadcast import Broadcast


class Tokenizer:
    """分词器类"""
    
    def __init__(self, spark_session=None, stopwords_path=None):
        """
        初始化分词器
        
        Args:
            spark_session: SparkSession对象
            stopwords_path: 停用词表路径
        """
        self.spark = spark_session
        self.stopwords = set()
        self.stopwords_broadcast = None
        
        # 加载停用词表
        if stopwords_path:
            self.load_stopwords(stopwords_path)
        else:
            # 使用默认路径
            default_path = Path(__file__).parent.parent.parent / "resources/dict/stopwords.txt"
            if default_path.exists():
                self.load_stopwords(str(default_path))
        
        # 注册UDF
        self._register_udfs()
    
    def load_stopwords(self, stopwords_path):
        """
        加载停用词表
        
        Args:
            stopwords_path: 停用词表路径
        """
        stopwords_path = Path(stopwords_path)
        
        if not stopwords_path.exists():
            print(f"警告: 停用词表不存在: {stopwords_path}")
            return
        
        with open(stopwords_path, 'r', encoding='utf-8') as f:
            self.stopwords = set(line.strip() for line in f if line.strip())
        
        print(f"已加载 {len(self.stopwords)} 个停用词")
    
    def broadcast_stopwords(self, spark_session):
        """
        广播停用词表到集群
        
        Args:
            spark_session: SparkSession对象
        """
        self.spark = spark_session
        self.stopwords_broadcast = spark_session.sparkContext.broadcast(self.stopwords)
        print("停用词表已广播到集群")
    
    def _register_udfs(self):
        """注册Spark UDF"""
        # 简单分词UDF（基于字符和标点分割）
        def simple_tokenize(text):
            if text is None:
                return []
            
            # 按标点符号分割
            tokens = re.split(r'[，。！？、；：""''（）\s]+', text)
            
            # 进一步分割长词（简单的基于字符的分词）
            result = []
            for token in tokens:
                if not token:
                    continue
                # 如果是纯英文或数字，保留整体
                if re.match(r'^[a-zA-Z0-9]+$', token):
                    result.append(token.lower())
                # 如果是中文，按2-4字符分词
                elif re.match(r'^[\u4e00-\u9fa5]+$', token):
                    # 简单的n-gram分词（2-4字）
                    if len(token) <= 4:
                        result.append(token)
                    else:
                        # 滑动窗口分词
                        for i in range(len(token) - 1):
                            result.append(token[i:i+2])
                else:
                    # 混合文本，按字符处理
                    if len(token) >= 2:
                        result.append(token)
            
            return result
        
        self.tokenize_udf = udf(simple_tokenize, ArrayType(StringType()))
        
        # 带停用词过滤的分词UDF
        stopwords_set = self.stopwords
        
        def tokenize_with_filter(text):
            if text is None:
                return []
            
            tokens = simple_tokenize(text)
            return [t for t in tokens if t and t not in stopwords_set and len(t) >= 2]
        
        self.tokenize_filter_udf = udf(tokenize_with_filter, ArrayType(StringType()))
    
    def tokenize(self, df, text_column='content', output_column='tokens'):
        """
        对文本进行分词
        
        Args:
            df: Spark DataFrame
            text_column: 输入文本列名
            output_column: 输出分词列名
            
        Returns:
            DataFrame: 包含分词结果的DataFrame
        """
        return df.withColumn(output_column, self.tokenize_udf(col(text_column)))
    
    def tokenize_and_filter(self, df, text_column='content', output_column='tokens'):
        """
        对文本进行分词并过滤停用词
        
        Args:
            df: Spark DataFrame
            text_column: 输入文本列名
            output_column: 输出分词列名
            
        Returns:
            DataFrame: 包含分词结果的DataFrame
        """
        return df.withColumn(output_column, self.tokenize_filter_udf(col(text_column)))
    
    def filter_stopwords(self, df, tokens_column='tokens', output_column=None):
        """
        过滤停用词
        
        Args:
            df: Spark DataFrame
            tokens_column: 分词列名
            output_column: 输出列名，如果为None则覆盖原列
            
        Returns:
            DataFrame: 过滤停用词后的DataFrame
        """
        if output_column is None:
            output_column = tokens_column
        
        stopwords_set = self.stopwords
        
        filter_udf = udf(
            lambda tokens: [t for t in tokens if t not in stopwords_set] if tokens else [],
            ArrayType(StringType())
        )
        
        return df.withColumn(output_column, filter_udf(col(tokens_column)))
    
    def remove_empty_tokens(self, df, tokens_column='tokens', min_tokens=1):
        """
        去除空的分词结果
        
        Args:
            df: Spark DataFrame
            tokens_column: 分词列名
            min_tokens: 最少分词数量
            
        Returns:
            DataFrame: 过滤后的DataFrame
        """
        return df.filter(size(col(tokens_column)) >= min_tokens)
    
    def tokens_to_string(self, df, tokens_column='tokens', output_column='tokens_str', 
                         separator=' '):
        """
        将分词列表转换为字符串
        
        Args:
            df: Spark DataFrame
            tokens_column: 分词列名
            output_column: 输出列名
            separator: 分隔符
            
        Returns:
            DataFrame: 包含分词字符串的DataFrame
        """
        from pyspark.sql.functions import concat_ws
        return df.withColumn(output_column, concat_ws(separator, col(tokens_column)))
    
    def get_word_count(self, df, tokens_column='tokens'):
        """
        统计词频
        
        Args:
            df: Spark DataFrame
            tokens_column: 分词列名
            
        Returns:
            DataFrame: 词频统计DataFrame
        """
        from pyspark.sql.functions import explode, count
        
        return df.select(explode(col(tokens_column)).alias('word')) \
            .groupBy('word') \
            .agg(count('*').alias('count')) \
            .orderBy(col('count').desc())


def main():
    """测试分词器"""
    from pyspark.sql import SparkSession
    
    # 创建SparkSession
    spark = SparkSession.builder \
        .appName("TokenizerTest") \
        .master("local[*]") \
        .getOrCreate()
    
    # 创建测试数据
    test_data = [
        ("1", "今天天气真好，心情也不错！"),
        ("2", "这个产品质量很好，推荐购买"),
        ("3", "服务态度太差了，非常不满意"),
        ("4", "价格有点贵，但质量还可以"),
    ]
    
    df = spark.createDataFrame(test_data, ["doc_id", "content"])
    
    # 创建分词器
    tokenizer = Tokenizer()
    
    # 分词
    df_tokenized = tokenizer.tokenize_and_filter(df)
    
    print("分词结果:")
    df_tokenized.show(truncate=False)
    
    # 词频统计
    print("\n词频统计:")
    word_count = tokenizer.get_word_count(df_tokenized)
    word_count.show(20)
    
    spark.stop()


if __name__ == "__main__":
    main()

