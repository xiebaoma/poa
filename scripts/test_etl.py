#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试ETL模块
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src/main/python"))

from data.generator import DataGenerator
from data.loader import DataLoader
from etl import ETLProcessor


def test_etl_module():
    """测试ETL模块"""
    print("=" * 60)
    print("ETL模块测试")
    print("=" * 60)
    
    # 步骤1：生成测试数据
    print("\n【步骤1】生成测试数据...")
    generator = DataGenerator()
    df_pandas = generator.generate_dataset(
        num_records=300,
        start_date="2025-01-01",
        end_date="2025-01-15",
        source_distribution={"weibo": 0.5, "product": 0.3, "news": 0.2}
    )
    print(f"✓ 已生成 {len(df_pandas)} 条测试数据")
    
    # 步骤2：加载数据到Spark
    print("\n【步骤2】加载数据到Spark...")
    loader = DataLoader()
    df = loader.load_raw_data(file_format='csv')
    print(f"✓ 已加载 {df.count()} 条数据到Spark")
    
    # 显示原始数据样本
    print("\n原始数据样本:")
    df.select('doc_id', 'content', 'source').show(5, truncate=False)
    
    # 步骤3：执行ETL处理
    print("\n【步骤3】执行ETL处理...")
    processor = ETLProcessor(spark_session=loader.spark)
    df_processed = processor.process(
        df,
        text_column='content',
        clean_text=True,
        deduplicate=True,
        tokenize=True,
        filter_stopwords=True,
        min_text_length=2,
        min_tokens=1
    )
    
    # 步骤4：查看处理结果
    print("\n【步骤4】处理结果预览...")
    print("\n处理后数据样本:")
    df_processed.select('doc_id', 'content', 'tokens_str', 'source').show(5, truncate=False)
    
    # 步骤5：查看统计信息
    print("\n【步骤5】统计信息...")
    processor.print_statistics(df_processed)
    
    # 步骤6：查看高频词
    print("\n【步骤6】高频词 Top-15:")
    top_words = processor.get_top_words(df_processed, top_n=15)
    top_words.show()
    
    # 步骤7：保存处理后的数据
    print("\n【步骤7】保存处理后的数据...")
    from utils.config_loader import get_data_paths
    paths = get_data_paths()
    output_path = paths['processed_path'] / "etl_test_output"
    
    df_processed.write.mode('overwrite').parquet(str(output_path))
    print(f"✓ 数据已保存到: {output_path}")
    
    print("\n" + "=" * 60)
    print("ETL模块测试完成！")
    print("=" * 60)
    
    return df_processed


def test_text_cleaner():
    """单独测试文本清洗器"""
    print("\n" + "=" * 60)
    print("文本清洗器测试")
    print("=" * 60)
    
    from pyspark.sql import SparkSession
    from etl import TextCleaner
    
    spark = SparkSession.builder \
        .appName("TextCleanerTest") \
        .master("local[*]") \
        .getOrCreate()
    
    # 创建包含噪音的测试数据
    test_data = [
        ("1", "这个产品真不错！https://example.com 推荐购买"),
        ("2", "@用户A 你觉得怎么样？[微笑][点赞]"),
        ("3", "#热门话题# 今天天气真好！！！"),
        ("4", "价格：￥99.9，质量还可以~~~"),
        ("5", "   多余空格   和   换行\n测试   "),
        ("6", "重复内容测试"),
        ("6", "重复内容测试"),  # 重复行
    ]
    
    df = spark.createDataFrame(test_data, ["doc_id", "content"])
    
    print("\n清洗前:")
    df.show(truncate=False)
    
    cleaner = TextCleaner()
    df_cleaned = cleaner.clean_pipeline(df, deduplicate=True)
    
    print("\n清洗后:")
    df_cleaned.show(truncate=False)
    
    spark.stop()


def test_tokenizer():
    """单独测试分词器"""
    print("\n" + "=" * 60)
    print("分词器测试")
    print("=" * 60)
    
    from pyspark.sql import SparkSession
    from etl import Tokenizer
    
    spark = SparkSession.builder \
        .appName("TokenizerTest") \
        .master("local[*]") \
        .getOrCreate()
    
    test_data = [
        ("1", "今天天气真好，心情也不错！"),
        ("2", "这个产品质量很好，推荐购买"),
        ("3", "服务态度太差了，非常不满意"),
        ("4", "价格有点贵，但质量还可以"),
        ("5", "系统运行稳定，用户体验很好"),
    ]
    
    df = spark.createDataFrame(test_data, ["doc_id", "content"])
    
    tokenizer = Tokenizer(spark)
    
    # 分词（带停用词过滤）
    df_tokenized = tokenizer.tokenize_and_filter(df)
    
    print("\n分词结果:")
    df_tokenized.show(truncate=False)
    
    # 词频统计
    print("\n词频统计 Top-10:")
    word_count = tokenizer.get_word_count(df_tokenized)
    word_count.show(10)
    
    spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL模块测试')
    parser.add_argument('--test', choices=['all', 'etl', 'cleaner', 'tokenizer'],
                        default='all', help='选择测试内容')
    args = parser.parse_args()
    
    try:
        if args.test == 'all':
            test_etl_module()
        elif args.test == 'etl':
            test_etl_module()
        elif args.test == 'cleaner':
            test_text_cleaner()
        elif args.test == 'tokenizer':
            test_tokenizer()
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()

