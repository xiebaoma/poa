# ETL模块使用说明

## 模块概述

ETL（Extract-Transform-Load）模块负责文本数据的预处理，包括：
1. **文本清洗** - 去除噪音数据
2. **分词处理** - 中文文本分词
3. **停用词过滤** - 去除无意义词汇
4. **去重处理** - 删除重复数据

## 核心组件

### 1. TextCleaner（文本清洗器）

负责去除文本中的噪音数据：
- URL链接
- 表情符号（如 [微笑]）
- @用户名
- 话题标签（#话题#）
- 特殊字符
- 多余空格

### 2. Tokenizer（分词器）

负责中文文本分词：
- 基于规则的简单分词
- 停用词过滤
- 词频统计

### 3. ETLProcessor（ETL处理器）

整合所有功能的完整处理流水线：
- 自动加载配置
- 管理SparkSession
- 提供统计信息

## 使用示例

### 快速开始

```python
from etl import ETLProcessor
from data.loader import DataLoader

# 加载原始数据
loader = DataLoader()
df = loader.load_raw_data()

# 创建ETL处理器
processor = ETLProcessor(spark_session=loader.spark)

# 执行ETL处理
df_processed = processor.process(df)

# 查看统计信息
processor.print_statistics(df_processed)

# 查看高频词
top_words = processor.get_top_words(df_processed, top_n=20)
top_words.show()
```

### 单独使用文本清洗器

```python
from etl import TextCleaner

cleaner = TextCleaner()

# 完整清洗流水线
df_cleaned = cleaner.clean_pipeline(
    df,
    text_column='content',
    deduplicate=True,
    remove_empty=True,
    min_length=2
)

# 或单独使用某个功能
df = cleaner.remove_urls(df)
df = cleaner.remove_emojis(df)
df = cleaner.deduplicate(df)
```

### 单独使用分词器

```python
from etl import Tokenizer

tokenizer = Tokenizer(spark_session)

# 分词（带停用词过滤）
df_tokenized = tokenizer.tokenize_and_filter(df, 'content', 'tokens')

# 词频统计
word_count = tokenizer.get_word_count(df_tokenized)
word_count.show(20)
```

### 保存处理结果

```python
# 保存为Parquet格式（推荐）
df_processed = processor.process_and_save(
    df,
    output_path="data/processed/output",
    output_format='parquet'
)
```

## 处理流程

```
原始数据
    ↓
┌─────────────────┐
│   文本清洗      │
│ - 去除URL       │
│ - 去除表情      │
│ - 去除@用户名   │
│ - 去除话题标签  │
│ - 规范化空格    │
└────────┬────────┘
         ↓
┌─────────────────┐
│   去重处理      │
│ - 基于内容去重  │
└────────┬────────┘
         ↓
┌─────────────────┐
│   分词处理      │
│ - 中文分词      │
│ - 停用词过滤    │
└────────┬────────┘
         ↓
处理后数据
```

## 输出字段

处理后的DataFrame包含以下字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| doc_id | String | 文档唯一标识符 |
| content | String | 清洗后的文本内容 |
| timestamp | Timestamp | 时间戳 |
| source | String | 数据源类型 |
| tokens | Array[String] | 分词结果列表 |
| tokens_str | String | 分词结果字符串（空格分隔） |
| processed_id | Long | 处理后的行ID |

## Spark技术点

本模块使用了以下Spark核心技术：

1. **DataFrame API**
   - `withColumn`: 添加/修改列
   - `filter`: 过滤数据
   - `dropDuplicates`: 去重

2. **UDF（用户自定义函数）**
   - 文本清洗UDF
   - 分词UDF

3. **Broadcast变量**
   - 广播停用词表到集群

4. **内置函数**
   - `regexp_replace`: 正则替换
   - `trim`: 去除空格
   - `length`: 计算长度
   - `size`: 计算数组大小
   - `explode`: 展开数组

## 配置说明

在 `config.yaml` 中配置：

```yaml
dictionary:
  stopwords: "src/main/resources/dict/stopwords.txt"
```

## 测试

```bash
# 运行完整测试
python scripts/test_etl.py

# 只测试文本清洗器
python scripts/test_etl.py --test cleaner

# 只测试分词器
python scripts/test_etl.py --test tokenizer
```

## 注意事项

1. **编码**: 所有文本使用UTF-8编码
2. **分词**: 当前使用简单的基于规则的分词，可扩展为jieba等专业分词库
3. **停用词**: 停用词表可根据业务需求自定义扩展
4. **性能**: 对于大数据量，建议使用Parquet格式保存中间结果

