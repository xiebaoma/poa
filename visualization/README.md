# 舆情分析系统 - Web可视化界面

## 📋 简介

基于 Streamlit 的舆情分析系统可视化界面，提供友好的交互式分析体验。

## ✨ 功能特性

### 1. 📥 数据处理
- **多格式支持**: CSV、JSON、Parquet
- **数据生成**: 内置模拟数据生成器
- **智能预处理**: 自动清洗、去重、分词、停用词过滤

### 2. 😊 情感分析
- **多维度展示**: 饼图、柱状图、统计卡片
- **情感分类**: 正面/中性/负面三分类
- **来源分析**: 按数据来源统计情感分布

### 3. 🔥 热点话题
- **高频词统计**: 词频排行Top-N
- **TF-IDF提取**: 关键词重要性分析
- **趋势识别**: 发现快速上升的热门词汇
- **可视化展示**: 柱状图、树状图

### 4. 📈 趋势分析
- **时间序列**: 情感随时间变化趋势
- **负面监控**: 负面情绪比例追踪
- **智能预警**: 自动识别异常舆情
- **多粒度聚合**: 支持小时/天/周/月维度

## 🚀 快速开始

### 前置要求

- Python 3.8+
- Java 8+ (用于PySpark)
- 2GB+ 内存

### 安装依赖

```bash
# 在项目根目录执行
pip install -r requirements.txt
```

### 启动方式

#### 方式1: 使用启动脚本 (推荐)

**Linux/Mac:**
```bash
# 添加执行权限
chmod +x visualization/run_web.sh

# 启动服务
./visualization/run_web.sh
```

**Windows:**
```cmd
# 双击运行或在命令行执行
visualization\run_web.bat
```

#### 方式2: 直接使用Streamlit命令

```bash
# 在项目根目录执行
streamlit run visualization/app_streamlit.py
```

### 访问界面

启动成功后，浏览器会自动打开，或手动访问:
```
http://localhost:8501
```

## 📖 使用指南

### 1. 配置参数

在左侧边栏配置分析参数:

#### 数据源配置
- **生成模拟数据**: 
  - 记录数: 100-10000条
  - 日期范围: 自定义起止日期
  
- **加载已有数据**:
  - 数据路径: 输入文件或目录路径
  - 文件格式: csv/json/parquet

#### 分析参数
- **Top-N关键词数量**: 5-50个
- **时间窗口**: hour/day/week/month

### 2. 运行分析

1. 配置完参数后，点击 **🚀 运行分析** 按钮
2. 等待分析完成（进度条显示）
3. 查看分析结果

### 3. 查看结果

分析完成后，在标签页中查看结果:

- **📋 数据概览**: 数据统计、样本预览
- **😊 情感分析**: 情感分布、按来源统计
- **🔥 热点话题**: 高频词、TF-IDF、趋势词
- **📈 趋势分析**: 时间序列、负面预警

### 4. 清除结果

点击左侧边栏的 **🗑️ 清除结果** 按钮可清除当前分析结果，重新配置参数。

## 🎨 界面预览

### 数据概览
- 统计卡片: 总记录数、情感分类数量
- 数据表格: 展示原始数据样本

### 情感分析
- 饼图: 情感分布比例
- 柱状图: 情感数量统计
- 分组柱状图: 各来源情感分布

### 热点话题
- 横向柱状图: 高频词Top-N
- 横向柱状图: TF-IDF关键词
- 树状图: 趋势上升词汇

### 趋势分析
- 折线图: 情感随时间变化
- 面积图: 负面情绪比例趋势
- 预警卡片: 负面舆情预警信息

## ⚙️ 配置说明

### 修改配置文件

编辑 `src/main/resources/config/config.yaml`:

```yaml
# 情感分析阈值
processing:
  trend:
    threshold_negative: 0.4  # 负面预警阈值

# Spark配置
spark:
  executor_memory: "2g"
  driver_memory: "1g"
```

### 自定义词典

编辑词典文件:
- `src/main/resources/dict/positive_words.txt`: 正面词汇
- `src/main/resources/dict/negative_words.txt`: 负面词汇
- `src/main/resources/dict/stopwords.txt`: 停用词

## 🔧 技术栈

- **前端框架**: Streamlit
- **可视化**: Plotly
- **数据处理**: PySpark
- **数据分析**: Pandas
- **编程语言**: Python 3.8+

## 📝 常见问题

### Q1: 启动时报错 "ModuleNotFoundError: No module named 'streamlit'"
**A**: 需要安装依赖包:
```bash
pip install streamlit plotly
```

### Q2: 分析过程中卡住不动
**A**: 可能是数据量太大，建议:
- 减少生成记录数
- 增加Spark内存配置
- 检查系统资源占用

### Q3: 无法访问8501端口
**A**: 端口可能被占用，可以修改启动脚本中的端口号:
```bash
streamlit run visualization/app_streamlit.py --server.port=8502
```

### Q4: 图表显示不完整
**A**: 刷新页面或清除浏览器缓存

## 📧 联系方式

如有问题或建议，请提交Issue或联系开发团队。

## 📄 许可证

本项目采用 MIT 许可证。

