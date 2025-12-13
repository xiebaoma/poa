#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试数据源层（数据生成器和加载器）
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src/main/python"))

from data.generator import DataGenerator
from data.loader import DataLoader


def test_data_generator():
    """测试数据生成器"""
    print("=" * 60)
    print("测试数据生成器")
    print("=" * 60)
    
    generator = DataGenerator()
    
    # 生成小规模测试数据
    print("\n生成100条测试数据...")
    df = generator.generate_dataset(
        num_records=100,
        start_date="2025-01-01",
        end_date="2025-01-31",
        source_distribution={"weibo": 0.4, "product": 0.4, "news": 0.2},
        format='csv'
    )
    
    print(f"\n生成的数据预览:")
    print(df.head(10))
    print(f"\n数据统计:")
    print(df['source'].value_counts())
    print(f"\n时间范围: {df['timestamp'].min()} 到 {df['timestamp'].max()}")
    
    return True


def test_data_loader():
    """测试数据加载器"""
    print("\n" + "=" * 60)
    print("测试数据加载器")
    print("=" * 60)
    
    loader = DataLoader()
    
    try:
        # 从配置的原始数据路径加载数据
        print("\n从配置路径加载CSV数据...")
        df = loader.load_raw_data(file_format='csv')
        
        # 显示DataFrame信息
        loader.print_dataframe_info(df, show_sample=True, sample_size=5)
        
        # 显示数据源分布
        print("\n数据源分布:")
        from pyspark.sql.functions import col
        source_counts = df.groupBy("source").count().orderBy(col("count").desc())
        source_counts.show()
        
        return True
    except FileNotFoundError as e:
        print(f"错误: {e}")
        print("请先运行数据生成器生成测试数据")
        return False
    except Exception as e:
        print(f"加载数据时出错: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("开始测试数据源层...\n")
    
    # 测试数据生成器
    try:
        test_data_generator()
    except Exception as e:
        print(f"数据生成器测试失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 测试数据加载器
    try:
        test_data_loader()
    except Exception as e:
        print(f"数据加载器测试失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 60)
    print("数据源层测试完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()

