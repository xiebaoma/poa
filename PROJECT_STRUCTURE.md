# 项目目录结构说明

## 整体结构

```
poa/
├── README.md                    # 项目说明文档
├── PROJECT_STRUCTURE.md         # 本文件：目录结构说明
├── requirements.txt             # Python依赖包
├── .gitignore                   # Git忽略文件配置
│
├── src/                         # 源代码目录
│   ├── main/
│   │   ├── python/              # Python源代码（PySpark）
│   │   │   ├── data/           # 数据源层
│   │   │   │   ├── generator/  # 数据生成器（模拟数据）
│   │   │   │   └── loader/     # 数据加载器（从文件/HDFS加载）
│   │   │   ├── etl/            # 模块一：文本数据预处理
│   │   │   ├── topic/          # 模块二：热点话题挖掘
│   │   │   ├── sentiment/      # 模块三：情感分析
│   │   │   ├── trend/          # 模块四：舆情趋势分析
│   │   │   ├── utils/          # 工具类模块
│   │   │   └── main/           # 主程序入口
│   │   └── resources/          # 资源文件
│   │       ├── dict/           # 词典文件
│   │       │   ├── stopwords.txt          # 停用词表
│   │       │   ├── positive_words.txt     # 正面情感词表
│   │       │   └── negative_words.txt     # 负面情感词表
│   │       └── config/         # 配置文件
│   │           └── config.yaml # 系统配置文件
│   └── test/                   # 测试代码
│       └── python/             # Python测试代码
│
├── data/                        # 数据目录
│   ├── raw/                    # 原始数据（CSV/JSON）
│   ├── processed/              # 处理后的数据（Parquet）
│   └── results/                # 分析结果数据
│
├── scripts/                     # 脚本文件
│   └── (运行脚本、工具脚本等)
│
└── visualization/               # 可视化相关（可选）
    └── (可视化代码、图表等)
```

## 各模块说明

### 1. 数据源层 (`src/main/python/data/`)

- **generator/**: 数据生成器
  - 用于生成模拟的文本数据（微博评论、商品评论、新闻等）
  - 便于测试和演示

- **loader/**: 数据加载器
  - 从CSV/JSON文件加载数据
  - 支持从HDFS加载数据
  - 数据格式：`doc_id, content, timestamp, source`

### 2. 模块一：文本预处理 (`src/main/python/etl/`)

- 数据清洗：去重、去噪（URL、表情、无意义字符）
- 分词处理
- 停用词过滤
- 输出清洗后的数据（Parquet格式）

### 3. 模块二：热点话题挖掘 (`src/main/python/topic/`)

- Word Count统计
- TF-IDF特征提取
- Top-N关键词提取
- 话题随时间变化趋势分析

### 4. 模块三：情感分析 (`src/main/python/sentiment/`)

- 基于情感词典的情感分析
- 情感得分计算（正面词 - 负面词）
- 分类：正面/负面/中性
- 支持MLlib Pipeline（可选）

### 5. 模块四：舆情趋势分析 (`src/main/python/trend/`)

- 按时间窗口聚合统计
- 计算每天/每小时正负面比例
- 负面舆情激增检测
- 趋势变化可视化数据准备

### 6. 工具类 (`src/main/python/utils/`)

- Spark工具函数
- 文本处理工具
- 配置加载工具
- 其他通用工具

### 7. 主程序 (`src/main/python/main/`)

- 系统主入口
- 协调各个模块的执行流程
- 参数解析和配置管理

### 8. 资源文件 (`src/main/resources/`)

- **dict/**: 词典文件
  - `stopwords.txt`: 停用词表
  - `positive_words.txt`: 正面情感词表
  - `negative_words.txt`: 负面情感词表

- **config/**: 配置文件
  - `config.yaml`: 系统配置（Spark参数、数据路径、处理参数等）

### 9. 数据目录 (`data/`)

- **raw/**: 存放原始输入数据（CSV/JSON格式）
- **processed/**: 存放ETL处理后的数据（Parquet格式）
- **results/**: 存放最终分析结果

## 开发顺序建议

1. **第一步**：数据源层（generator + loader）
2. **第二步**：模块一（ETL文本预处理）
3. **第三步**：模块三（情感分析）- 核心功能
4. **第四步**：模块二（热点话题挖掘）
5. **第五步**：模块四（舆情趋势分析）
6. **第六步**：主程序整合
7. **第七步**：可视化（可选）

## 技术栈

- **计算引擎**: Apache Spark (PySpark)
- **数据处理**: Spark SQL / DataFrame
- **NLP**: Spark MLlib
- **特征提取**: TF-IDF
- **存储格式**: CSV (输入) → Parquet (中间) → CSV/MySQL (输出)
- **可视化**: Python + matplotlib (可选)

