"""
分词器
精简版实现，专注于单次 UDF 调用以提高稳定性
"""
import re
from pyspark.sql.functions import udf, col, size, concat_ws
from pyspark.sql.types import ArrayType, StringType


class Tokenizer:
    """精简版分词器"""
    
    def __init__(self, spark_session=None, stopwords_path=None):
        self.spark = spark_session
        self.stopwords = set()
        
        if stopwords_path:
            try:
                with open(stopwords_path, 'r', encoding='utf-8') as f:
                    self.stopwords = set(line.strip() for line in f if line.strip())
                print(f"已加载 {len(self.stopwords)} 个停用词")
            except Exception as e:
                print(f"停用词加载失败: {e}")

    def _get_tokenize_func(self):
        """创建闭包分词函数，减少 UDF 序列化开销"""
        stops = self.stopwords
        
        def tokenize(text):
            if text is None: return []
            try:
                # 简单分词逻辑：按非中文字符拆分
                # 注意：实际生产中建议使用 jieba，此处为演示精简逻辑
                tokens = re.split(r'[^\u4e00-\u9fa5a-zA-Z0-9]+', str(text))
                # 过滤停用词和单字
                return [t.lower() for t in tokens if len(t) > 1 and t not in stops][:100]
            except:
                return []
        return tokenize

    def tokenize_and_filter(self, df, input_col, output_col='tokens'):
        """分词并过滤停用词"""
        tokenize_udf = udf(self._get_tokenize_func(), ArrayType(StringType()))
        return df.withColumn(output_col, tokenize_udf(col(input_col)))

    def remove_empty_tokens(self, df, tokens_column='tokens', min_tokens=1):
        """去除空分词结果"""
        return df.filter(size(col(tokens_column)) >= min_tokens)

    def tokens_to_string(self, df, tokens_column='tokens', output_column='tokens_str'):
        """数组转字符串"""
        return df.withColumn(output_column, concat_ws(' ', col(tokens_column)))

    def broadcast_stopwords(self, spark):
        """由于目前使用闭包 UDF，此方法仅为兼容旧代码"""
        pass
