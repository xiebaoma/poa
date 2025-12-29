"""
ETL处理器 - 重构精简版
解决 Windows 环境下的稳定性问题 (EOFError)
"""
from pathlib import Path
import os
import sys
import platform
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import monotonically_increasing_id

# ==========================================
# Windows 极致稳定性补丁
# ==========================================
if platform.system() == 'Windows':
    # 强制 Python 解释器路径使用正斜杠
    py_exe = sys.executable.replace('\\', '/')
    os.environ['PYSPARK_PYTHON'] = py_exe
    os.environ['PYSPARK_DRIVER_PYTHON'] = py_exe
    
    # 彻底解决 EOFError: 禁用 Worker 复用和守护进程
    os.environ['PYSPARK_NO_DAEMON'] = '1'
    os.environ['PYTHONUNBUFFERED'] = '1'

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).parent.parent))

from .text_cleaner import TextCleaner
from .tokenizer import Tokenizer


class ETLProcessor:
    """重构版 ETL 处理器"""
    
    def __init__(self, spark_session=None, config=None):
        # 1. 配置加载
        if config is None:
            from utils.config_loader import load_config
            config = load_config()
        self.config = config
        
        # 2. Spark 初始化 (针对 Windows 优化)
        if spark_session is None:
            master = config.get('spark', {}).get('master', 'local[1]')
            if platform.system() == 'Windows':
                master = 'local[1]' # Windows 强制单线程运行最稳定
                
            self.spark = SparkSession.builder \
                .appName("SentimentAnalysis_ETL") \
                .master(master) \
                .config("spark.python.worker.reuse", "false") \
                .config("spark.sql.shuffle.partitions", "1") \
                .getOrCreate()
        else:
            self.spark = spark_session

        # 3. 组件初始化
        self.cleaner = TextCleaner()
        
        # 查找停用词路径
        project_root = Path(__file__).parent.parent.parent.parent.parent
        stops_path = project_root / config.get('dictionary', {}).get('stopwords', 'resources/dict/stopwords.txt')
        self.tokenizer = Tokenizer(self.spark, str(stops_path) if stops_path.exists() else None)

    def process(self, df: DataFrame, text_column='content', **kwargs) -> DataFrame:
        """执行精简 ETL 流水线"""
        print("\n>>> 开始 ETL 处理 (精简重构版)")
        
        # 步骤1: 文本清洗 (使用原生 Spark 算子，极稳)
        print("步骤1: 执行原生文本清洗...")
        df = self.cleaner.clean_pipeline(df, text_column, deduplicate=True)
        
        # 步骤2: 分词处理 (单一防御式 UDF)
        print("步骤2: 执行分词处理...")
        df = self.tokenizer.tokenize_and_filter(df, text_column, 'tokens')
        df = self.tokenizer.remove_empty_tokens(df, 'tokens')
        df = self.tokenizer.tokens_to_string(df, 'tokens', 'tokens_str')
        
        # 步骤3: 生成 ID
        df = df.withColumn("processed_id", monotonically_increasing_id())
        
        print(">>> ETL 处理完成\n")
        return df.cache()

    def print_statistics(self, df, **kwargs):
        """保持接口兼容，打印简单统计"""
        print(f"最终处理数据量: {df.count()}")


if __name__ == "__main__":
    # 冒烟测试
    processor = ETLProcessor()
    test_df = processor.spark.createDataFrame([
        ("1", "这是一个测试数据！@用户 [微笑] http://t.cn/abc"),
        ("2", "质量非常差，不推荐。"),
        ("3", "质量非常差，不推荐。") # 测试去重
    ], ["doc_id", "content"])
    
    result = processor.process(test_df)
    result.show(truncate=False)
