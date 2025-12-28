# 舆情分析系统环境搭建指南

## 一、系统要求

### 1.1 操作系统
- **推荐**：Linux (Ubuntu 20.04+ / CentOS 7+) 或 macOS
- **可选**：Windows 10/11（需要额外配置）

### 1.2 硬件要求（最低配置）
- **CPU**：2核心以上（推荐4核心+）
- **内存**：4GB以上（推荐8GB+，Spark默认配置需要2GB executor + 1GB driver）
- **磁盘**：至少10GB可用空间（用于数据存储和Spark临时文件）

### 1.3 软件要求
- **Java**：JDK 8 或 JDK 11（Spark必需）
- **Python**：3.8、3.9、3.11 或 3.12（推荐3.11）
  - ⚠️ **注意**：Python 3.13 在 Windows 上与 PySpark 存在兼容性问题，建议使用 3.11 或 3.12
- **Spark**：3.5.0+（通过PySpark自动安装）
  - ⚠️ **Windows 用户**：请使用 PySpark 3.5.x 版本，避免使用 4.0+ 版本（详见下方说明）

---

## 二、环境搭建步骤

### 2.1 安装 Java（必需）

#### Linux/macOS
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install openjdk-11-jdk

# CentOS/RHEL
sudo yum install java-11-openjdk-devel

# macOS (使用Homebrew)
brew install openjdk@11

# 验证安装
java -version
# 应该显示：openjdk version "11.x.x"
```

#### Windows
1. 下载 JDK 11：https://adoptium.net/
2. 安装并配置环境变量 `JAVA_HOME`
3. 添加到 PATH

#### 设置 JAVA_HOME
```bash
# Linux/macOS - 添加到 ~/.bashrc 或 ~/.zshrc
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # 路径可能不同
export PATH=$JAVA_HOME/bin:$PATH

# 验证
echo $JAVA_HOME
```

---

### 2.2 安装 Python（必需）

⚠️ **重要提示 - Windows 用户**：
- 如果你在 Windows 上，建议使用 **Python 3.11** 或 **Python 3.12**
- **避免使用 Python 3.13**，因为它与 PySpark 存在兼容性问题
- 详见：[PYSPARK_WINDOWS_FIX.md](PYSPARK_WINDOWS_FIX.md)

#### Linux
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install python3.9 python3.9-pip python3.9-venv

# CentOS/RHEL
sudo yum install python39 python39-pip
```

#### macOS
```bash
# 使用Homebrew
brew install python@3.9
```

#### Windows
**推荐安装 Python 3.11 或 3.12：**
1. 从 https://www.python.org/downloads/ 下载 Python 3.11 或 3.12
2. 安装时勾选 "Add Python to PATH"
3. ⚠️ **避免使用 Python 3.13**（PySpark 兼容性问题）

#### 验证安装
```bash
python3 --version  # 或 python --version
# 应该显示：Python 3.9.x
pip3 --version     # 或 pip --version
```

---

### 2.3 创建 Python 虚拟环境（推荐）

```bash
# 进入项目目录
cd /path/to/poa

# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
# Linux/macOS:
source venv/bin/activate

# Windows:
venv\Scripts\activate

# 验证（命令行前应显示 (venv)）
which python  # Linux/macOS
where python  # Windows
```

---

### 2.4 安装 Python 依赖包

```bash
# 确保虚拟环境已激活
pip install --upgrade pip

# 安装项目依赖
pip install -r requirements.txt
```

#### 依赖包说明
- **pyspark>=3.5.0,<4.0.0**：Spark Python API（会自动下载Spark）
  - ⚠️ **Windows 用户注意**：已限制版本为 3.5.x，因为 PySpark 4.x 在 Windows 上存在兼容性问题
  - 详见：[PYSPARK_WINDOWS_FIX.md](PYSPARK_WINDOWS_FIX.md)
- **pandas>=2.0.0**：数据处理（数据生成器使用）
- **numpy>=1.24.0**：数值计算
- **pyyaml>=6.0**：配置文件解析
- **matplotlib>=3.7.0**：可视化（可选）
- **seaborn>=0.12.0**：可视化（可选）

#### 验证安装
```bash
python -c "import pyspark; print(pyspark.__version__)"
# 应该显示：3.5.0 或更高版本
```

---

### 2.5 配置 Spark（自动配置）

**好消息**：PySpark 会自动下载和管理 Spark，无需手动安装！

但如果你想手动配置或使用集群模式：

#### 方式1：使用PySpark自动管理（推荐，默认）
- PySpark 会自动下载 Spark 到用户目录
- 无需额外配置

#### 方式2：手动安装 Spark（可选，用于集群模式）

```bash
# 下载 Spark
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark

# 配置环境变量
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH

# 添加到 ~/.bashrc 或 ~/.zshrc
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
```

---

### 2.6 验证环境

创建测试脚本 `test_env.py`：

```python
import sys
print(f"Python版本: {sys.version}")

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("EnvTest") \
        .master("local[*]") \
        .getOrCreate()
    print("✓ Spark环境正常")
    print(f"Spark版本: {spark.version}")
    spark.stop()
except Exception as e:
    print(f"✗ Spark环境异常: {e}")

try:
    import pandas as pd
    import numpy as np
    import yaml
    print("✓ Python依赖包正常")
except Exception as e:
    print(f"✗ 依赖包异常: {e}")
```

运行测试：
```bash
python test_env.py
```

---

## 三、运行模式配置

### 3.1 本地模式（默认，推荐用于开发/测试）

配置文件：`src/main/resources/config/config.yaml`

```yaml
spark:
  master: "local[*]"  # 使用所有可用CPU核心
  executor_memory: "2g"
  driver_memory: "1g"
```

**特点**：
- 单机运行，无需集群
- 适合开发、测试、小规模数据
- 自动使用所有CPU核心

### 3.2 集群模式（可选，用于生产环境）

```yaml
spark:
  master: "spark://master-host:7077"  # Spark集群Master地址
  executor_memory: "4g"
  driver_memory: "2g"
```

**前提条件**：
- 已搭建Spark集群
- 配置好HDFS（可选，用于分布式存储）

---

## 四、数据存储配置

### 4.1 本地文件系统（默认）

系统默认使用本地文件系统：
- **原始数据**：`data/raw/`
- **处理后数据**：`data/processed/`
- **结果数据**：`data/results/`

**无需额外配置**，直接使用即可。

### 4.2 HDFS（可选，用于大规模数据）

如果需要使用HDFS：

1. **安装Hadoop**（如果还没有）
2. **配置HDFS路径**：修改 `config.yaml`
   ```yaml
   data:
     raw_path: "hdfs://namenode:9000/data/raw"
     processed_path: "hdfs://namenode:9000/data/processed"
     results_path: "hdfs://namenode:9000/data/results"
   ```

---

## 五、可选组件

### 5.1 MySQL（可选，用于结果存储）

如果需要将结果保存到MySQL：

1. **安装MySQL**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install mysql-server
   
   # macOS
   brew install mysql
   ```

2. **安装Python驱动**
   ```bash
   pip install pymysql
   ```

3. **配置数据库**
   ```yaml
   output:
     format: "mysql"
     mysql:
       host: "localhost"
       port: 3306
       database: "sentiment_analysis"
       table: "results"
   ```

4. **创建数据库**
   ```sql
   CREATE DATABASE sentiment_analysis;
   ```

### 5.2 可视化工具（可选）

系统支持使用 matplotlib/seaborn 进行可视化：

```bash
pip install matplotlib seaborn
```

---

## 六、快速启动测试

### 6.1 生成测试数据

```bash
# 方式1：使用主程序
python -m src.main.python.main.app --generate -n 100

# 方式2：直接运行生成器
python -m src.main.python.data.generator.data_generator
```

### 6.2 运行完整流水线

```bash
# 生成数据并分析
python -m src.main.python.main.app --generate -n 1000

# 或从已有文件分析
python -m src.main.python.main.app -i data/raw/data.csv
```

---

## 七、常见问题排查

### 7.1 Java版本问题
```
错误：UnsupportedClassVersionError
解决：确保使用 JDK 8 或 11，不要使用 JDK 17+
```

### 7.2 Spark内存不足
```
错误：OutOfMemoryError
解决：在 config.yaml 中降低 executor_memory 和 driver_memory
```

### 7.3 找不到模块
```
错误：ModuleNotFoundError
解决：确保虚拟环境已激活，并已安装 requirements.txt
```

### 7.4 路径问题
```
错误：FileNotFoundError
解决：确保在项目根目录运行，或使用绝对路径
```

### 7.5 Windows 上的 PySpark 兼容性问题 ⚠️

**问题描述**：
```
错误：AttributeError: module 'socketserver' has no attribute 'UnixStreamServer'
或：PySpark 在 Windows 上无法启动
```

**原因**：
- PySpark 4.x 在 Windows 上与 Python 3.13 存在兼容性问题
- `UnixStreamServer` 类在 Windows 上不存在（仅限 Unix/Linux）

**解决方案**：

**方案 1：使用 PySpark 3.5.x（推荐）✓**
```bash
pip uninstall pyspark -y
pip install "pyspark>=3.5.0,<4.0.0"
```

**方案 2：使用兼容性补丁**
项目已包含自动兼容性补丁，如果需要使用 PySpark 4.x：
```bash
# 运行兼容性检查
python pyspark_windows_compat.py

# 补丁已自动集成到主程序中
python src/main/python/main/app.py
```

**方案 3：降级 Python 版本**
```bash
# 卸载 Python 3.13
# 安装 Python 3.11 或 3.12
```

**详细说明**：请参阅 [PYSPARK_WINDOWS_FIX.md](PYSPARK_WINDOWS_FIX.md)

---

## 八、环境检查清单

在运行系统前，请确认：

- [ ] Java 已安装（`java -version`）
- [ ] Python 3.8+ 已安装（`python --version`）
- [ ] 虚拟环境已创建并激活
- [ ] 所有依赖包已安装（`pip list`）
- [ ] Spark 可以正常启动（运行 `test_env.py`）
- [ ] 项目目录结构完整（`data/raw/`, `data/processed/`, `data/results/` 存在）
- [ ] 词典文件存在（`src/main/resources/dict/*.txt`）

---

## 九、最小化环境（仅核心功能）

如果只需要运行核心功能，最小依赖：

```bash
pip install pyspark>=3.5.0 pandas>=2.0.0 pyyaml>=6.0
```

**不需要**：
- numpy（如果不用高级数据处理）
- matplotlib/seaborn（如果不用可视化）
- jieba（如果使用内置简单分词）
- pymysql（如果不用MySQL）

---

## 十、生产环境建议

如果部署到生产环境：

1. **使用集群模式**：配置 Spark Standalone 或 YARN
2. **使用HDFS**：存储大规模数据
3. **监控**：配置 Spark UI 监控任务执行
4. **日志**：配置日志系统（如 Log4j）
5. **资源管理**：根据数据量调整 executor 数量和内存

---

## 总结

**最简单的运行方式**（适合课程项目/本地测试）：

1. 安装 Java 11
2. 安装 Python 3.9
3. 创建虚拟环境并安装依赖
4. 直接运行（使用本地模式）

**无需额外配置**：
- 无需手动安装 Spark（PySpark自动管理）
- 无需配置HDFS（使用本地文件系统）
- 无需配置数据库（使用文件输出）

系统设计为"开箱即用"，适合快速上手和演示！

