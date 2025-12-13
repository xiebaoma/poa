# 数据源层使用说明

## 模块概述

数据源层包含两个核心模块：
1. **数据生成器（DataGenerator）**：生成模拟的文本数据
2. **数据加载器（DataLoader）**：从文件加载数据到Spark DataFrame

## 数据格式

所有数据遵循以下格式：

| 字段 | 类型 | 说明 |
|------|------|------|
| doc_id | String | 文档唯一标识符 |
| content | String | 文本内容 |
| timestamp | Timestamp | 时间戳 |
| source | String | 数据源类型（weibo/product/news） |

## 1. 数据生成器（DataGenerator）

### 功能
- 生成模拟的微博评论、商品评论、新闻文本
- 支持自定义数据量、时间范围、数据源分布
- 输出CSV或JSON格式

### 使用示例

```python
from data.generator import DataGenerator

# 创建生成器
generator = DataGenerator()

# 生成1000条数据
df = generator.generate_dataset(
    num_records=1000,
    start_date="2025-01-01",
    end_date="2025-01-31",
    source_distribution={"weibo": 0.4, "product": 0.4, "news": 0.2},
    format='csv'
)
```

### 参数说明

- `num_records`: 生成记录数（默认1000）
- `start_date`: 开始日期，格式"YYYY-MM-DD"（默认30天前）
- `end_date`: 结束日期，格式"YYYY-MM-DD"（默认今天）
- `source_distribution`: 数据源分布字典，如 `{"weibo": 0.5, "product": 0.3, "news": 0.2}`
- `output_path`: 输出文件路径（默认使用配置的raw_path）
- `format`: 输出格式，'csv' 或 'json'（默认'csv'）

## 2. 数据加载器（DataLoader）

### 功能
- 从CSV/JSON/Parquet文件加载数据
- 支持本地文件系统和HDFS
- 自动创建SparkSession
- 提供数据信息查看功能

### 使用示例

```python
from data.loader import DataLoader

# 创建加载器（自动创建SparkSession）
loader = DataLoader()

# 方式1：从配置的原始数据路径加载所有CSV文件
df = loader.load_raw_data(file_format='csv')

# 方式2：从指定文件加载
df = loader.load_from_csv("data/raw/raw_data_20250101.csv")

# 方式3：从JSON文件加载
df = loader.load_from_json("data/raw/raw_data.json")

# 方式4：从Parquet文件加载
df = loader.load_from_parquet("data/processed/processed_data.parquet")

# 查看数据信息
loader.print_dataframe_info(df)
```

### 主要方法

#### `load_from_csv(file_path, schema=None, header=True, infer_schema=False)`
从CSV文件加载数据

#### `load_from_json(file_path, schema=None, multi_line=False)`
从JSON文件加载数据

#### `load_from_parquet(file_path)`
从Parquet文件加载数据

#### `load_from_directory(directory_path, file_format='csv', **kwargs)`
从目录加载多个文件

#### `load_raw_data(file_path=None, file_format='csv')`
从配置的原始数据路径加载数据

#### `print_dataframe_info(df, show_sample=True, sample_size=10)`
打印DataFrame的基本信息和样本数据

## 测试

运行测试脚本：

```bash
python scripts/test_data_layer.py
```

或者单独测试：

```bash
# 测试数据生成器
python -m src.main.python.data.generator.data_generator

# 测试数据加载器
python -m src.main.python.data.loader.data_loader
```

## 注意事项

1. **Spark环境**：确保已安装PySpark，数据加载器会自动创建SparkSession
2. **配置文件**：系统会从 `src/main/resources/config/config.yaml` 读取配置
3. **数据路径**：默认数据路径在 `data/raw/` 目录下
4. **编码格式**：所有文件使用UTF-8编码

