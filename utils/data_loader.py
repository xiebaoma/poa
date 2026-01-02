# -*- coding: utf-8 -*-
"""
数据加载工具

数据集：COVID-19 微博情感数据集
"""
import os
import sys
import pandas as pd

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# 数据集文件名
COVID_WEIBO_FILE = "covid_weibo_train.csv"


def load_covid_weibo_data(file_path=None):
    """
    加载 COVID-19 微博情感数据集
    
    数据来源: Hugging Face souljoy/COVID-19_weibo_emotion
    特点: 真实疫情微博数据，包含6类情感标签
    日期范围: 2020-01-20 ~ 2020-04-30
    
    Returns:
        DataFrame with columns: id, date, content, label, source, emotion
    """
    if file_path is None:
        file_path = os.path.join(config.DATA_DIR, COVID_WEIBO_FILE)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"数据集不存在: {file_path}\n请确保 data 目录下有 covid_weibo_train.csv 文件")
    
    print(f"正在加载数据: {file_path}")
    df = pd.read_csv(file_path)
    
    # 标准化列名
    df = df.rename(columns={
        'text': 'content',
        'label_name': 'emotion',
        'label': 'label'
    })
    
    # 添加ID列
    df['id'] = range(1, len(df) + 1)
    
    # 添加来源
    df['source'] = '微博-疫情'
    
    # 将情感标签转换为数值
    emotion_to_label = {
        'happy': 1,
        'neutral': -1,
        'angry': 0,
        'sad': 0,
        'fear': 0,
        'surprise': -1
    }
    df['sentiment_label'] = df['emotion'].map(emotion_to_label)
    
    # 确保列顺序
    columns = ['id', 'date', 'content', 'label', 'emotion', 'sentiment_label', 'source']
    df = df[[col for col in columns if col in df.columns]]
    
    print(f"数据加载完成，共 {len(df)} 条记录")
    
    return df


def load_custom_data(file_path):
    """
    加载自定义CSV数据
    
    要求CSV至少包含以下列：
    - date: 日期（格式：YYYY-MM-DD）
    - content: 文本内容
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"数据文件不存在: {file_path}")
    
    print(f"正在加载自定义数据: {file_path}")
    
    # 尝试不同编码
    encodings = ['utf-8', 'gbk', 'gb2312', 'utf-8-sig']
    df = None 
    
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
    
    if df is None:
        raise ValueError("无法解析CSV文件，请检查文件编码")
    
    # 检查必要列
    if 'content' not in df.columns and 'text' not in df.columns:
        possible_content_cols = ['content', 'text', 'review', 'comment', '内容', '文本', '评论']
        for col in possible_content_cols:
            if col in df.columns:
                df = df.rename(columns={col: 'content'})
                break
    
    if 'content' not in df.columns:
        raise ValueError("CSV文件必须包含 'content' 或 'text' 列")
    
    if 'date' not in df.columns:
        possible_date_cols = ['date', 'time', 'datetime', 'created_at', '日期', '时间']
        for col in possible_date_cols:
            if col in df.columns:
                df = df.rename(columns={col: 'date'})
                break
    
    if 'date' not in df.columns:
        raise ValueError("CSV文件必须包含 'date' 列")
    
    # 添加ID列
    df['id'] = range(1, len(df) + 1)
    
    # 设置默认值
    if 'label' not in df.columns:
        df['label'] = None
    
    if 'source' not in df.columns:
        df['source'] = '自定义数据'
    
    # 确保列顺序
    columns = ['id', 'date', 'content', 'label', 'source']
    df = df[[col for col in columns if col in df.columns]]
    
    print(f"数据加载完成，共 {len(df)} 条记录")
    return df


def get_date_range(df):
    """获取数据集的日期范围"""
    if 'date' not in df.columns:
        return None, None
    
    dates = pd.to_datetime(df['date'], errors='coerce')
    return dates.min(), dates.max()


def filter_by_date(df, start_date=None, end_date=None):
    """按日期范围筛选数据"""
    if 'date' not in df.columns:
        return df
    
    df_copy = df.copy()
    df_copy['_date'] = pd.to_datetime(df_copy['date'], errors='coerce')
    
    try:
        if start_date:
            start_date = pd.to_datetime(start_date)
            df_copy = df_copy[df_copy['_date'] >= start_date]
        
        if end_date:
            end_date = pd.to_datetime(end_date)
            df_copy = df_copy[df_copy['_date'] <= end_date]
    except Exception as e:
        print(f"日期格式错误: {e}")
        # 返回未筛选的数据
        return df
    
    return df_copy.drop(columns=['_date'])


if __name__ == "__main__":
    # 测试数据加载
    print("=" * 50)
    print("加载 COVID-19 微博情感数据集")
    print("=" * 50)
    
    df = load_covid_weibo_data()
    print(f"\n数据集列: {list(df.columns)}")
    print(f"日期范围: {get_date_range(df)}")
    print(f"\n前5条数据:")
    print(df.head())
