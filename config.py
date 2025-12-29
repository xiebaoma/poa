# -*- coding: utf-8 -*-
"""
配置文件
"""
import os

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# 数据目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# 数据文件路径
DATA_FILE = os.path.join(DATA_DIR, "weibo_senti_100k.csv")
STOPWORDS_FILE = os.path.join(DATA_DIR, "stopwords.txt")

# Spark配置
SPARK_APP_NAME = "HotWordAnalysis"
SPARK_MASTER = "local[1]"  # 本地单线程

# 分析配置
DEFAULT_TOP_N = 20  # 默认显示Top N热词
MIN_WORD_LENGTH = 2  # 最小词长度

# 情感分析配置
POSITIVE_THRESHOLD = 0.6  # 正面情感阈值
NEGATIVE_THRESHOLD = 0.4  # 负面情感阈值

# GUI配置
WINDOW_WIDTH = 1400
WINDOW_HEIGHT = 900
WINDOW_TITLE = "Python+Spark 热词分析系统"

# 词云配置
WORDCLOUD_WIDTH = 800
WORDCLOUD_HEIGHT = 400
WORDCLOUD_FONT = os.path.join(DATA_DIR, "simhei.ttf")  # 中文字体
WORDCLOUD_BG_COLOR = "white"
