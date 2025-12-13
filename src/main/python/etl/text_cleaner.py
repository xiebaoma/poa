"""
文本清洗器
用于去噪、去重等文本预处理操作
"""
import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, trim, lower, regexp_replace, length
from pyspark.sql.types import StringType


class TextCleaner:
    """文本清洗器类"""
    
    def __init__(self):
        """初始化文本清洗器"""
        # URL正则表达式
        self.url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        
        # 表情符号正则表达式（包括emoji和中文表情标签）
        self.emoji_pattern = r'\[[\u4e00-\u9fa5a-zA-Z0-9]+\]'
        
        # @用户名正则表达式
        self.mention_pattern = r'@[\w\u4e00-\u9fa5]+'
        
        # 话题标签正则表达式
        self.hashtag_pattern = r'#[^#]+#'
        
        # 特殊字符（保留中文、英文、数字和基本标点）
        self.special_char_pattern = r'[^\u4e00-\u9fa5a-zA-Z0-9，。！？、；：""''（）\s]+'
        
        # 多余空格
        self.multi_space_pattern = r'\s+'
        
        # 注册UDF
        self._register_udfs()
    
    def _register_udfs(self):
        """注册Spark UDF"""
        # 综合清洗UDF
        def clean_text_func(text):
            if text is None:
                return ""
            
            # 去除URL
            text = re.sub(self.url_pattern, '', text)
            
            # 去除表情符号
            text = re.sub(self.emoji_pattern, '', text)
            
            # 去除@用户名
            text = re.sub(self.mention_pattern, '', text)
            
            # 去除话题标签
            text = re.sub(self.hashtag_pattern, '', text)
            
            # 去除特殊字符
            text = re.sub(self.special_char_pattern, '', text)
            
            # 去除多余空格
            text = re.sub(self.multi_space_pattern, ' ', text)
            
            # 去除首尾空格
            text = text.strip()
            
            return text
        
        self.clean_text_udf = udf(clean_text_func, StringType())
    
    def remove_urls(self, df, text_column='content'):
        """
        去除URL
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            
        Returns:
            DataFrame: 处理后的DataFrame
        """
        return df.withColumn(
            text_column,
            regexp_replace(col(text_column), self.url_pattern, '')
        )
    
    def remove_emojis(self, df, text_column='content'):
        """
        去除表情符号
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            
        Returns:
            DataFrame: 处理后的DataFrame
        """
        return df.withColumn(
            text_column,
            regexp_replace(col(text_column), self.emoji_pattern, '')
        )
    
    def remove_mentions(self, df, text_column='content'):
        """
        去除@用户名
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            
        Returns:
            DataFrame: 处理后的DataFrame
        """
        return df.withColumn(
            text_column,
            regexp_replace(col(text_column), self.mention_pattern, '')
        )
    
    def remove_hashtags(self, df, text_column='content'):
        """
        去除话题标签
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            
        Returns:
            DataFrame: 处理后的DataFrame
        """
        return df.withColumn(
            text_column,
            regexp_replace(col(text_column), self.hashtag_pattern, '')
        )
    
    def remove_special_chars(self, df, text_column='content'):
        """
        去除特殊字符
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            
        Returns:
            DataFrame: 处理后的DataFrame
        """
        return df.withColumn(
            text_column,
            regexp_replace(col(text_column), self.special_char_pattern, '')
        )
    
    def normalize_whitespace(self, df, text_column='content'):
        """
        规范化空白字符
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            
        Returns:
            DataFrame: 处理后的DataFrame
        """
        return df.withColumn(
            text_column,
            trim(regexp_replace(col(text_column), self.multi_space_pattern, ' '))
        )
    
    def clean_text(self, df, text_column='content'):
        """
        综合文本清洗
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            
        Returns:
            DataFrame: 清洗后的DataFrame
        """
        return df.withColumn(text_column, self.clean_text_udf(col(text_column)))
    
    def remove_empty_rows(self, df, text_column='content', min_length=2):
        """
        去除空行和过短的文本
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            min_length: 最小文本长度
            
        Returns:
            DataFrame: 过滤后的DataFrame
        """
        return df.filter(
            (col(text_column).isNotNull()) & 
            (length(col(text_column)) >= min_length)
        )
    
    def deduplicate(self, df, subset=None, keep='first'):
        """
        去重
        
        Args:
            df: Spark DataFrame
            subset: 用于去重的列名列表，如果为None则使用'content'列
            keep: 保留方式，'first'保留第一个
            
        Returns:
            DataFrame: 去重后的DataFrame
        """
        if subset is None:
            subset = ['content']
        
        return df.dropDuplicates(subset)
    
    def clean_pipeline(self, df, text_column='content', deduplicate=True, 
                       remove_empty=True, min_length=2):
        """
        完整的清洗流水线
        
        Args:
            df: Spark DataFrame
            text_column: 文本列名
            deduplicate: 是否去重
            remove_empty: 是否去除空行
            min_length: 最小文本长度
            
        Returns:
            DataFrame: 清洗后的DataFrame
        """
        # 统计原始数据量
        original_count = df.count()
        print(f"原始数据量: {original_count}")
        
        # 1. 综合文本清洗
        df = self.clean_text(df, text_column)
        print(f"文本清洗完成")
        
        # 2. 去除空行
        if remove_empty:
            df = self.remove_empty_rows(df, text_column, min_length)
            count_after_empty = df.count()
            print(f"去除空行后: {count_after_empty} (删除 {original_count - count_after_empty} 行)")
        
        # 3. 去重
        if deduplicate:
            count_before_dedup = df.count()
            df = self.deduplicate(df, [text_column])
            count_after_dedup = df.count()
            print(f"去重后: {count_after_dedup} (删除 {count_before_dedup - count_after_dedup} 重复行)")
        
        return df

