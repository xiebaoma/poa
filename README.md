# 舆情分析系统 (Public Opinion Analysis System)

## 🔴 首次运行必读！

### ⚠️ 必须安装 Java！

**如果你看到 "系统找不到指定的路径" 错误，说明你没有安装 Java！**

PySpark 需要 Java 才能运行。请先安装：

1. **下载 Java 17**：https://adoptium.net/ （选择 Windows .msi 安装包）
2. **安装时勾选**："Add to PATH" 和 "Set JAVA_HOME"
3. **验证安装**：重新打开终端，运行 `java -version`

**详细说明**：[JAVA_REQUIRED.md](JAVA_REQUIRED.md)

---

## ⚠️ Windows + Python 3.13 用户重要提示

如果你在 **Windows + Python 3.13** 环境下遇到 PySpark 兼容性问题：

### 问题 1: distutils 模块不存在（Python 3.13）
**错误信息**: `ModuleNotFoundError: No module named 'distutils'`

**快速解决**:
```cmd
# 自动修复（推荐）
python fix_python313.py

# 或手动安装依赖
pip install setuptools packaging
pip install -r requirements.txt
```

**详细说明**: [PYTHON313_FIX.md](PYTHON313_FIX.md)

### 问题 2: UnixStreamServer 不存在（Windows + PySpark 4.x）
**快速解决**：
```cmd
# 运行自动安装脚本
install_windows.bat

# 或手动安装兼容版本
pip install "pyspark>=3.5.0,<4.0.0"
```

**详细说明**：[PYSPARK_WINDOWS_FIX.md](PYSPARK_WINDOWS_FIX.md) 或 [PYSPARK_FIX_SUMMARY.md](PYSPARK_FIX_SUMMARY.md)

### 推荐配置 ✓
- **Python**: 3.11 或 3.12（避免 3.13）
- **PySpark**: 3.5.x（已在 requirements.txt 中配置）

---

## 🚀 快速开始

### 方式1: Web可视化界面 (推荐)

提供友好的Web界面，无需编写代码即可进行舆情分析。

```bash
# 安装依赖
pip install -r requirements.txt

# 启动Web界面
./visualization/run_web.sh        # Mac/Linux
# 或
visualization\run_web.bat          # Windows

# 访问 http://localhost:8501
```

📖 详细文档: [WEB_VISUALIZATION.md](WEB_VISUALIZATION.md)

### 方式2: 命令行使用

```bash
# 生成模拟数据并分析
python src/main/python/main/app.py --generate --num-records 1000

# 分析已有数据
python src/main/python/main/app.py --input data/raw --format csv
```

---

# 一、这个应用到底"有什么用"？

## 1️⃣ 舆情 / 文本分析在真实世界的用途

### 📌 政府 / 公共管理

* 监测 **社会舆情走势**
* 提前发现：

  * 群体性事件风险
  * 负面舆情爆发点
* 示例：

  > 某政策发布后，网民情绪是正面还是负面？变化趋势如何？

---

### 📌 企业 / 产品 / 品牌

* 品牌口碑分析
* 产品问题预警
* 用户真实反馈挖掘

**典型问题：**

* 用户对某产品的情绪是变好还是变差？
* 哪些功能被吐槽最多？
* 新版本上线后负面评论是否激增？

---

### 📌 金融 / 投资

* 新闻情绪 → 股价预测辅助
* 风险舆情识别

> 比如：突然出现大量负面新闻 → 风险提示

---

### 📌 平台内容治理

* 发现恶意内容
* 发现情绪极端用户
* 话题热度监控

---

### 📌 学术 & 数据分析

* 社会情绪研究
* 舆论传播模型

---

# 二、系统整体功能设计（重点）


---

## 模块一：文本数据预处理（ETL）

### 📥 输入数据

* 微博评论
* 商品评论
* 新闻文本
* 模拟生成也可以

### 🔧 处理内容

* 去重
* 去噪（URL、表情、无意义字符）
* 分词（中文可以简单按词典或空格模拟）
* 停用词过滤

### ⭐ Spark 技术点

* RDD / DataFrame
* map / flatMap
* broadcast 停用词表

---

## 模块二：热点话题挖掘

### 🎯 目标

找出：

* 热门词
* 热门话题
* 话题随时间变化趋势

### 🔧 方法

* Word Count
* TF-IDF
* Top-N 关键词

### ⭐ Spark 技术点

* reduceByKey
* groupBy
* Spark SQL
* window（按时间）

### 📊 输出示例

```
2025-04-01  热点词：AI、裁员、房价
2025-04-02  热点词：高考、就业
```

---

## 模块三：情感分析（项目亮点）

### 🎯 目标

判断文本是：

* 正面
* 负面
* 中性

### 🔧 实现方式（课程友好）

#### ✅ 方案 1：情感词典法（推荐）

* 正面词表
* 负面词表
* 情感得分 = 正面词 - 负面词

### ⭐ Spark 技术点

* MLlib Pipeline
* UDF
* 向量化处理

---

## 模块四：舆情趋势分析（老师最爱）

### 🎯 分析内容

* 情绪随时间变化
* 负面舆情是否突然激增

### 🔧 实现

* 按时间窗口聚合
* 计算每天 / 每小时正负面比例

### 📈 输出

```
时间        正面    负面    中性
4-1         60%     20%     20%
4-2         45%     40%     15%
```

---

非常好，这一步你已经在**按“工程项目”而不是“课程作业”来思考了** 👍
下面我给你一套**「课程友好 + 工业常见 + 容易讲清楚」**的**整体架构 + 技术选型方案**，你几乎可以直接画成一张架构图放进 PPT。

---



# 一、整体架构概览（先给结论）

这是我**最推荐你用的三层架构**：

```
┌────────────┐
│  数据源层  │  微博 / 评论 / 新闻 / 模拟文本
└─────┬──────┘
      ↓
┌────────────┐
│  存储层    │  HDFS / 本地文件系统
└─────┬──────┘
      ↓
┌────────────┐
│  计算层    │  Spark（核心）
│  - ETL     │
│  - NLP     │
│  - 情感分析│
│  - 统计分析│
└─────┬──────┘
      ↓
┌────────────┐
│  结果层    │  CSV / Parquet / MySQL
└────────────┘
```

> **核心思想：**
> 用 Spark 对海量非结构化文本做离线批处理分析。

---

# 二、分层架构详细说明（答辩重点）

## ① 数据源层（Data Source）

### 📌 数据来源（老师最宽容）

你可以任选：

* 微博 / 商品评论（公开数据集）
* 新闻文本
* 自己模拟生成（完全 OK）

### 📂 数据格式（推荐）

```text
doc_id, content, timestamp, source
```

### 💡 为什么这样设计？

* content：文本主体
* timestamp：做趋势分析
* source：区分平台

---

## ② 存储层（Storage Layer）

### 🔧 技术选型

* **HDFS（推荐）**
* 本地文件系统（如果环境受限）

### 📦 文件格式

| 阶段   | 格式            | 原因            |
| ---- | ------------- | ------------- |
| 原始数据 | CSV / JSON    | 易生成           |
| 清洗后  | Parquet       | 列式存储，Spark 友好 |
| 结果   | CSV / Parquet | 易展示           |

### 📌 答辩用一句话

> 使用 Parquet 格式减少 I/O，提高 Spark 扫描效率。

---

## ③ 计算层（核心：Spark）

### 🔥 技术选型（重点）

| 模块    | 技术                    |
| ----- | --------------------- |
| 分布式计算 | Apache Spark          |
| 数据处理  | DataFrame / Spark SQL |
| NLP   | Spark MLlib           |
| 分词    | UDF / 自定义逻辑           |
| 机器学习  | TF-IDF / LR / NB      |

---

### 🔧 计算层内部架构（重点画）

```
         Spark Driver
              │
      ┌───────┴────────┐
      │   Spark ETL    │
      │ 清洗 / 去噪    │
      └───────┬────────┘
              ↓
      ┌────────────────┐
      │  特征提取模块  │
      │ TF-IDF / 词频  │
      └───────┬────────┘
              ↓
      ┌────────────────┐
      │  情感分析模块  │
      │ 词典 / MLlib   │
      └───────┬────────┘
              ↓
      ┌────────────────┐
      │ 趋势统计模块   │
      │ 时间窗口分析   │
      └────────────────┘
```

---

### ⭐ Spark 用到的核心能力（你可以强调）

* 分布式并行处理
* shuffle & join
* broadcast 变量（情感词典）
* cache 中间结果
* ML Pipeline

---

## ④ 结果层（Result Layer）

### 🔧 技术选型

* CSV / Parquet（课程最稳）
* MySQL（加分项）
* 可选：简单可视化（Python）

### 📊 输出示例

* 热点关键词 Top-N
* 每日情感分布
* 舆情趋势变化

---

## ⑤（可选）展示层 / 可视化

不是必须，但**加分**。

### 技术选型

* Python + matplotlib
* Jupyter Notebook
* 简单 Web 页面

---

# 三、完整技术选型总结表（你可以直接放 PPT）

| 层次  | 技术选型                  | 说明       |
| --- | --------------------- | -------- |
| 数据源 | CSV / JSON            | 简单通用     |
| 存储  | HDFS / 本地             | Spark 兼容 |
| 计算  | Apache Spark          | 核心       |
| API | Spark SQL / DataFrame | 易优化      |
| NLP | Spark MLlib           | 工业常见     |
| 特征  | TF-IDF                | 经典文本特征   |
| 模型  | LR / NB               | 轻量模型     |
| 结果  | CSV / MySQL           | 易展示      |

---

# 四、为什么这套架构“合理”？（老师爱听）

你可以总结成 3 点：

1️⃣ **分层清晰**：解耦数据、计算、结果
2️⃣ **技术选型合理**：Spark 适合批量文本处理
3️⃣ **可扩展性强**：

* 批处理 → 流处理
* 词典 → 机器学习 → 深度学习

---
