"""
舆情分析系统主程序
整合数据加载、ETL预处理、情感分析、话题挖掘、趋势分析的完整流水线
"""
import argparse
from pathlib import Path
from datetime import datetime
import sys
import platform
import os

# 添加项目路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root / "src/main/python"))

# Windows 兼容性补丁：在导入 PySpark 之前应用
if platform.system() == 'Windows':
    try:
        # 尝试导入并应用兼容性补丁
        pyspark_compat_path = project_root / "pyspark_windows_compat.py"
        if pyspark_compat_path.exists():
            sys.path.insert(0, str(project_root))
            from pyspark_windows_compat import apply_all_patches
            apply_all_patches()
        
        # 修复 Spark Windows 路径问题
        spark_fix_path = project_root / "fix_spark_windows.py"
        if spark_fix_path.exists():
            from fix_spark_windows import fix_spark_windows_paths
            fix_spark_windows_paths()
    except Exception as e:
        print(f"警告: 无法应用 PySpark Windows 兼容性补丁: {e}", file=sys.stderr)

# Python 3.13 兼容性补丁：修复 distutils 依赖
if sys.version_info >= (3, 12):
    try:
        from pyspark_windows_compat import apply_python313_patch
        apply_python313_patch()
    except Exception as e:
        print(f"警告: 无法应用 Python 3.13 兼容性补丁: {e}", file=sys.stderr)

from pyspark.sql import SparkSession

from utils.config_loader import load_config, get_data_paths
from data.generator import DataGenerator
from data.loader import DataLoader
from etl import ETLProcessor
from sentiment import SentimentAnalyzer
from topic import TopicMiner
from trend import TrendAnalyzer


class SentimentAnalysisApp:
    """舆情分析系统主应用"""
    
    def __init__(self, config=None):
        """
        初始化应用
        
        Args:
            config: 配置字典，如果为None则自动加载
        """
        if config is None:
            config = load_config()
        
        self.config = config
        self.spark = None
        self.paths = get_data_paths(config)
        
        # 初始化各模块（延迟初始化）
        self._loader = None
        self._etl_processor = None
        self._sentiment_analyzer = None
        self._topic_miner = None
        self._trend_analyzer = None
    
    def _init_spark(self):
        """初始化SparkSession"""
        if self.spark is None:
            print("初始化 Spark...")
            spark_config = self.config.get('spark', {})
            
            try:
                builder = SparkSession.builder \
                    .appName(spark_config.get('app_name', 'SentimentAnalysisSystem')) \
                    .master(spark_config.get('master', 'local[*]'))
                
                # 基本配置 - 使用较小的内存避免问题
                builder = builder \
                    .config("spark.executor.memory", "1g") \
                    .config("spark.driver.memory", "1g") \
                    .config("spark.sql.shuffle.partitions", "2") \
                    .config("spark.default.parallelism", "2") \
                    .config("spark.python.worker.reuse", "false") \
                    .config("spark.python.worker.memory", "512m")
                
                # Windows 特定配置 - 完全禁用 Hadoop
                if platform.system() == 'Windows':
                    print("应用 Windows 配置...")
                    project_root = Path(__file__).parent.parent.parent.parent.resolve()
                    
                    # 使用用户目录下的临时目录
                    temp_dir = Path.home() / "spark-temp"
                    temp_dir.mkdir(parents=True, exist_ok=True)
                    
                    # 设置 Hadoop 目录并下载 winutils（使用项目根目录，不是src目录）
                    hadoop_home = project_root / ".hadoop"
                    hadoop_home.mkdir(parents=True, exist_ok=True)
                    hadoop_bin = hadoop_home / "bin"
                    hadoop_bin.mkdir(parents=True, exist_ok=True)
                    
                    # 尝试下载 winutils.exe
                    print("  检查 winutils.exe...")
                    try:
                        from fix_spark_windows import download_winutils
                        download_winutils(hadoop_home)
                    except Exception as e:
                        print(f"  ⚠️  下载 winutils 时出错: {e}")
                    
                    # 设置环境变量（必须在 Spark 启动前设置）
                    # 使用 Windows 格式的绝对路径，并添加 bin 目录到 PATH
                    hadoop_home_str = str(hadoop_home.resolve()).replace('/', '\\')
                    hadoop_bin_str = str(hadoop_bin.resolve()).replace('/', '\\')
                    
                    os.environ['HADOOP_USER_NAME'] = 'spark'
                    os.environ['HADOOP_HOME'] = hadoop_home_str
                    
                    # 关键：将 hadoop bin 目录添加到 PATH，让 JVM 能找到 hadoop.dll
                    current_path = os.environ.get('PATH', '')
                    if hadoop_bin_str not in current_path:
                        os.environ['PATH'] = f"{hadoop_bin_str};{current_path}"
                    
                    print(f"  ✓ HADOOP_HOME: {hadoop_home_str}")
                    print(f"  ✓ 已添加 bin 到 PATH: {hadoop_bin_str}")
                    
                    # Java 17+ 兼容性参数
                    java_opts = [
                        f"-Djava.io.tmpdir={temp_dir}",
                        f"-Dhadoop.home.dir={hadoop_home_str}",
                        "-DHADOOP_USER_NAME=spark",
                        "--add-opens=java.base/java.lang=ALL-UNNAMED",
                        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
                        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
                        "--add-opens=java.base/java.io=ALL-UNNAMED",
                        "--add-opens=java.base/java.net=ALL-UNNAMED",
                        "--add-opens=java.base/java.nio=ALL-UNNAMED",
                        "--add-opens=java.base/java.util=ALL-UNNAMED",
                        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
                        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
                        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
                        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
                        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
                        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
                        "-XX:+IgnoreUnrecognizedVMOptions"
                    ]
                    java_opts_str = " ".join(java_opts)
                    
                    # Spark 配置
                    builder = builder \
                        .config("spark.hadoop.hadoop.security.authentication", "simple") \
                        .config("spark.hadoop.hadoop.security.authorization", "false") \
                        .config("spark.authenticate", "false") \
                        .config("spark.sql.warehouse.dir", str((project_root / "spark-warehouse").resolve())) \
                        .config("spark.sql.catalogImplementation", "in-memory") \
                        .config("spark.driver.host", "localhost") \
                        .config("spark.local.dir", str((project_root / ".spark-local").resolve())) \
                        .config("spark.driver.extraJavaOptions", java_opts_str) \
                        .config("spark.executor.extraJavaOptions", java_opts_str) \
                        .config("spark.sql.session.timeZone", "UTC") \
                        .config("spark.ui.enabled", "false")
                    
                    # 禁用一些可能导致路径问题的功能
                    builder = builder \
                        .config("spark.sql.hive.metastore.sharedPrefixes", "") \
                        .config("spark.sql.hive.metastore.barrierPrefixes", "") \
                        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                    
                    print("✓ Windows 配置已应用")
                    print(f"✓ 临时目录: {temp_dir}")
                    print(f"✓ Hadoop 用户: spark")
                
                print("正在启动 Spark...")
                self.spark = builder.getOrCreate()
                
                # 设置日志级别为 WARN 减少输出
                self.spark.sparkContext.setLogLevel("WARN")
                
                print("✓ Spark 启动成功")
                
            except Exception as e:
                print(f"\n✗ Spark 启动失败!")
                print(f"错误: {e}")
                print("\n尝试诊断...")
                
                # 检查 Java
                import subprocess
                try:
                    result = subprocess.run(['java', '-version'], 
                                          capture_output=True, 
                                          text=True, 
                                          timeout=5)
                    print(f"✓ Java 可用")
                    # 打印 Java 版本信息
                    version_output = result.stderr if result.stderr else result.stdout
                    print(f"  版本: {version_output.split()[2] if version_output.split() else 'unknown'}")
                except Exception as java_err:
                    print(f"✗ Java 不可用或不在 PATH 中: {java_err}")
                
                # 检查环境变量
                print(f"\n环境变量:")
                print(f"  JAVA_HOME: {os.environ.get('JAVA_HOME', '未设置')}")
                print(f"  HADOOP_HOME: {os.environ.get('HADOOP_HOME', '未设置')}")
                print(f"  SPARK_LOCAL_DIRS: {os.environ.get('SPARK_LOCAL_DIRS', '未设置')}")
                
                print("\n完整错误:")
                import traceback
                traceback.print_exc()
                
                raise RuntimeError(f"无法启动 Spark: {e}") from e
                
        return self.spark
    
    @property
    def loader(self):
        if self._loader is None:
            print("创建 DataLoader...")
            self._loader = DataLoader(self._init_spark())
            print("DataLoader 创建完成")
        return self._loader
    
    @property
    def etl_processor(self):
        if self._etl_processor is None:
            self._etl_processor = ETLProcessor(self._init_spark(), self.config)
        return self._etl_processor
    
    @property
    def sentiment_analyzer(self):
        if self._sentiment_analyzer is None:
            self._sentiment_analyzer = SentimentAnalyzer(self._init_spark())
        return self._sentiment_analyzer
    
    @property
    def topic_miner(self):
        if self._topic_miner is None:
            self._topic_miner = TopicMiner(self._init_spark(), self.config)
        return self._topic_miner
    
    @property
    def trend_analyzer(self):
        if self._trend_analyzer is None:
            self._trend_analyzer = TrendAnalyzer(self._init_spark(), self.config)
        return self._trend_analyzer
    
    def generate_data(self, num_records=1000, start_date=None, end_date=None):
        """
        生成模拟数据
        
        Args:
            num_records: 生成记录数
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            str: 生成的数据文件路径
        """
        print("\n" + "=" * 60)
        print("【步骤1】生成模拟数据")
        print("=" * 60)
        
        generator = DataGenerator()
        df = generator.generate_dataset(
            num_records=num_records,
            start_date=start_date,
            end_date=end_date
        )
        
        return df
    
    def load_data(self, file_path=None, file_format='csv'):
        """
        加载数据
        
        Args:
            file_path: 数据文件路径
            file_format: 文件格式
            
        Returns:
            DataFrame: 加载的数据
        """
        print("\n" + "=" * 60)
        print("【步骤1】加载数据")
        print("=" * 60)
        
        try:
            if file_path:
                df = self.loader.load_from_directory(file_path, file_format)
            else:
                df = self.loader.load_raw_data(file_format=file_format)
            
            count = df.count()
            print(f"已加载 {count} 条数据")
            return df
        except Exception as e:
            print(f"加载数据时出错: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def preprocess(self, df):
        """
        ETL预处理
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            DataFrame: 预处理后的数据
        """
        print("\n" + "=" * 60)
        print("【步骤2】ETL预处理")
        print("=" * 60)
        
        df_processed = self.etl_processor.process(
            df,
            text_column='content',
            clean_text=True,
            deduplicate=True,
            tokenize=True,
            filter_stopwords=True
        )
        
        return df_processed
    
    def analyze_sentiment(self, df):
        """
        情感分析
        
        Args:
            df: 预处理后的DataFrame
            
        Returns:
            DataFrame: 包含情感分析结果的DataFrame
        """
        print("\n" + "=" * 60)
        print("【步骤3】情感分析")
        print("=" * 60)
        
        df_sentiment = self.sentiment_analyzer.analyze(df, tokens_column='tokens')
        self.sentiment_analyzer.print_statistics(df_sentiment)
        
        return df_sentiment
    
    def mine_topics(self, df, top_n=20):
        """
        热点话题挖掘
        
        Args:
            df: 包含分词结果的DataFrame
            top_n: Top-N关键词数量
            
        Returns:
            dict: 话题挖掘结果
        """
        print("\n" + "=" * 60)
        print("【步骤4】热点话题挖掘")
        print("=" * 60)
        
        self.topic_miner.print_topic_summary(df, top_n=top_n)
        
        results = {
            'top_words': self.topic_miner.get_top_words(df, top_n=top_n),
            'tfidf_keywords': self.topic_miner.get_top_tfidf_words(df, top_n=top_n),
            'trending': self.topic_miner.get_trending_words(df, top_n=10)
        }
        
        return results
    
    def analyze_trend(self, df, time_window='day'):
        """
        舆情趋势分析
        
        Args:
            df: 包含情感分析结果的DataFrame
            time_window: 时间窗口
            
        Returns:
            dict: 趋势分析结果
        """
        print("\n" + "=" * 60)
        print("【步骤5】舆情趋势分析")
        print("=" * 60)
        
        self.trend_analyzer.print_trend_summary(df, time_window=time_window)
        
        results = {
            'sentiment_by_time': self.trend_analyzer.sentiment_by_time(df, time_window=time_window),
            'negative_alerts': self.trend_analyzer.detect_negative_surge(df, time_window=time_window)
        }
        
        return results
    
    def save_results(self, df, topic_results, trend_results, output_dir=None):
        """
        保存分析结果
        
        Args:
            df: 完整的分析结果DataFrame
            topic_results: 话题挖掘结果
            trend_results: 趋势分析结果
            output_dir: 输出目录
        """
        print("\n" + "=" * 60)
        print("【步骤6】保存分析结果")
        print("=" * 60)
        
        if output_dir is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir = self.paths['results_path'] / f"analysis_{timestamp}"
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存完整结果
        result_path = output_dir / "full_results"
        df.select('doc_id', 'content', 'source', 'timestamp', 
                  'tokens_str', 'sentiment_label', 'sentiment_score',
                  'positive_count', 'negative_count') \
            .write.mode('overwrite').parquet(str(result_path))
        print(f"✓ 完整结果已保存到: {result_path}")
        
        # 保存热点词
        top_words_path = output_dir / "top_words.csv"
        topic_results['top_words'].toPandas().to_csv(str(top_words_path), index=False)
        print(f"✓ 热点词已保存到: {top_words_path}")
        
        # 保存TF-IDF关键词
        tfidf_path = output_dir / "tfidf_keywords.csv"
        topic_results['tfidf_keywords'].toPandas().to_csv(str(tfidf_path), index=False)
        print(f"✓ TF-IDF关键词已保存到: {tfidf_path}")
        
        # 保存趋势数据
        trend_path = output_dir / "sentiment_trend.csv"
        trend_results['sentiment_by_time'].toPandas().to_csv(str(trend_path), index=False)
        print(f"✓ 情感趋势已保存到: {trend_path}")
        
        # 保存预警数据
        if trend_results['negative_alerts'].count() > 0:
            alerts_path = output_dir / "negative_alerts.csv"
            trend_results['negative_alerts'].toPandas().to_csv(str(alerts_path), index=False)
            print(f"✓ 负面预警已保存到: {alerts_path}")
        
        print(f"\n所有结果已保存到: {output_dir}")
    
    def run(self, input_path=None, file_format='csv', generate=False, 
            num_records=1000, start_date=None, end_date=None,
            top_n=20, time_window='day', save=True, output_dir=None):
        """
        运行完整的分析流水线
        
        Args:
            input_path: 输入数据路径
            file_format: 文件格式
            generate: 是否生成模拟数据
            num_records: 生成记录数
            start_date: 开始日期
            end_date: 结束日期
            top_n: Top-N关键词数量
            time_window: 时间窗口
            save: 是否保存结果
            output_dir: 输出目录
            
        Returns:
            dict: 分析结果
        """
        print("\n" + "=" * 60)
        print("舆情分析系统")
        print("=" * 60)
        print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 步骤1：加载或生成数据
        if generate:
            self.generate_data(num_records, start_date, end_date)
            df = self.load_data(file_format=file_format)
        else:
            df = self.load_data(input_path, file_format)
        
        # 步骤2：ETL预处理
        df_processed = self.preprocess(df)
        
        # 缓存预处理结果
        df_processed = df_processed.cache()
        
        # 步骤3：情感分析
        df_sentiment = self.analyze_sentiment(df_processed)
        
        # 缓存情感分析结果
        df_sentiment = df_sentiment.cache()
        
        # 步骤4：热点话题挖掘
        topic_results = self.mine_topics(df_sentiment, top_n)
        
        # 步骤5：舆情趋势分析
        trend_results = self.analyze_trend(df_sentiment, time_window)
        
        # 步骤6：保存结果
        if save:
            self.save_results(df_sentiment, topic_results, trend_results, output_dir)
        
        print("\n" + "=" * 60)
        print("分析完成！")
        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        return {
            'data': df_sentiment,
            'topics': topic_results,
            'trends': trend_results
        }
    
    def stop(self):
        """停止SparkSession"""
        if self.spark:
            self.spark.stop()
            self.spark = None


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='舆情分析系统')
    
    # 数据源参数
    parser.add_argument('--input', '-i', type=str, help='输入数据路径')
    parser.add_argument('--format', '-f', type=str, default='csv', 
                        choices=['csv', 'json', 'parquet'], help='输入文件格式')
    parser.add_argument('--generate', '-g', action='store_true', help='生成模拟数据')
    parser.add_argument('--num-records', '-n', type=int, default=1000, help='生成记录数')
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    
    # 分析参数
    parser.add_argument('--top-n', type=int, default=20, help='Top-N关键词数量')
    parser.add_argument('--time-window', '-t', type=str, default='day',
                        choices=['hour', 'day', 'week', 'month'], help='时间窗口')
    
    # 输出参数
    parser.add_argument('--output', '-o', type=str, help='输出目录')
    parser.add_argument('--no-save', action='store_true', help='不保存结果')
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()
    
    app = SentimentAnalysisApp()
    
    try:
        app.run(
            input_path=args.input,
            file_format=args.format,
            generate=args.generate,
            num_records=args.num_records,
            start_date=args.start_date,
            end_date=args.end_date,
            top_n=args.top_n,
            time_window=args.time_window,
            save=not args.no_save,
            output_dir=args.output
        )
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        app.stop()


if __name__ == "__main__":
    main()

