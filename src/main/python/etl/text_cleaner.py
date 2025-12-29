"""
文本清洗器
使用原生 Spark 算子进行高性能清洗，避免 Python UDF 带来的稳定性问题
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, trim, length


class TextCleaner:
    """精简版文本清洗器"""
    
    def __init__(self):
        # 定义核心清理规则（正则表达式）
        self.rules = [
            (r'https?://[^\s<>"{}|\\^`\[\]]+', ''),     # URL
            (r'\[[\u4e00-\u9fa5a-zA-Z0-9]+\]', ''),     # 表情标签如 [微笑]
            (r'@[\w\u4e00-\u9fa5]+', ''),               # @用户名
            (r'#[^#]+#', ''),                           # 话题标签
            (r'[^\u4e00-\u9fa5a-zA-Z0-9，。！？、；：""''（）\s]+', ''), # 特殊符号
            (r'\s+', ' ')                               # 合并多余空格
        ]
    
    def clean(self, df: DataFrame, text_column: str = 'content') -> DataFrame:
        """
        执行流水线清洗
        使用原生 Spark 算子，不涉及 Python 进程切换，极速且稳定
        """
        # 应用正则规则
        for pattern, replacement in self.rules:
            df = df.withColumn(text_column, regexp_replace(col(text_column), pattern, replacement))
        
        # 去除首尾空格，并过滤掉太短的文本
        df = df.withColumn(text_column, trim(col(text_column))) \
               .filter(length(col(text_column)) >= 2)
        
        return df

    def clean_pipeline(self, df, text_column='content', deduplicate=True, **kwargs):
        """兼容旧接口的流水线调用"""
        print(f"执行文本清洗: {text_column}")
        df = self.clean(df, text_column)
        
        if deduplicate:
            df = df.dropDuplicates([text_column])
            
        return df.cache()
