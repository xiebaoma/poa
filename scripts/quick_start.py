#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
快速开始示例：演示如何使用数据生成器和加载器
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src/main/python"))

from data.generator import DataGenerator
from data.loader import DataLoader


def quick_start_example():
    """快速开始示例"""
    print("=" * 60)
    print("舆情分析系统 - 数据源层快速开始")
    print("=" * 60)
    
    # 步骤1：生成测试数据
    print("\n【步骤1】生成测试数据...")
    generator = DataGenerator()
    
    df_pandas = generator.generate_dataset(
        num_records=500,  # 生成500条数据
        start_date="2025-01-01",
        end_date="2025-01-15",
        source_distribution={"weibo": 0.5, "product": 0.3, "news": 0.2},
        format='csv'
    )
    
    print(f"✓ 已生成 {len(df_pandas)} 条数据")
    print(f"✓ 数据源分布: {df_pandas['source'].value_counts().to_dict()}")
    
    # 步骤2：加载数据到Spark
    print("\n【步骤2】加载数据到Spark DataFrame...")
    loader = DataLoader()
    
    df_spark = loader.load_raw_data(file_format='csv')
    
    print("✓ 数据已加载到Spark")
    
    # 步骤3：查看数据信息
    print("\n【步骤3】查看数据信息...")
    loader.print_dataframe_info(df_spark, show_sample=True, sample_size=5)
    
    # 步骤4：简单统计
    print("\n【步骤4】数据统计...")
    from pyspark.sql.functions import count, col
    
    # 按数据源统计
    print("\n按数据源统计:")
    df_spark.groupBy("source").agg(count("*").alias("count")).orderBy(col("count").desc()).show()
    
    # 按日期统计
    print("\n按日期统计（前10天）:")
    from pyspark.sql.functions import date_format
    df_spark.withColumn("date", date_format("timestamp", "yyyy-MM-dd")) \
        .groupBy("date") \
        .agg(count("*").alias("count")) \
        .orderBy("date") \
        .show(10)
    
    print("\n" + "=" * 60)
    print("快速开始示例完成！")
    print("=" * 60)
    print("\n下一步：可以开始实现ETL模块进行文本预处理")


if __name__ == "__main__":
    try:
        quick_start_example()
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()

