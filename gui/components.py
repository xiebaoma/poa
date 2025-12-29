# -*- coding: utf-8 -*-
"""
GUI 组件模块

包含词云、图表等可视化组件
"""
import os
import sys
from typing import List, Dict, Tuple, Optional
import io

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# 图形库
try:
    import matplotlib
    matplotlib.use('TkAgg')  # 使用 Tk 后端
    import matplotlib.pyplot as plt
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("警告: matplotlib 未安装")

# 词云
try:
    from wordcloud import WordCloud
    WORDCLOUD_AVAILABLE = True
except ImportError:
    WORDCLOUD_AVAILABLE = False
    print("警告: wordcloud 未安装")

# PIL
try:
    from PIL import Image, ImageTk
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("警告: Pillow 未安装")

import tkinter as tk
from tkinter import ttk


def get_chinese_font_path() -> Optional[str]:
    """获取中文字体路径"""
    # 常见中文字体路径
    font_paths = [
        config.WORDCLOUD_FONT,
        "C:/Windows/Fonts/simhei.ttf",
        "C:/Windows/Fonts/msyh.ttc",
        "C:/Windows/Fonts/simsun.ttc",
        "/System/Library/Fonts/PingFang.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
    ]
    
    for path in font_paths:
        if os.path.exists(path):
            return path
    
    return None


def generate_wordcloud(word_freq: Dict[str, int], 
                        width: int = None,
                        height: int = None,
                        bg_color: str = None) -> Optional[Image.Image]:
    """
    生成词云图片
    
    Args:
        word_freq: 词频字典 {word: count, ...}
        width: 宽度
        height: 高度
        bg_color: 背景颜色
    
    Returns:
        PIL Image 对象
    """
    if not WORDCLOUD_AVAILABLE:
        print("wordcloud 不可用")
        return None
    
    if not word_freq:
        return None
    
    width = width or config.WORDCLOUD_WIDTH
    height = height or config.WORDCLOUD_HEIGHT
    bg_color = bg_color or config.WORDCLOUD_BG_COLOR
    
    font_path = get_chinese_font_path()
    
    try:
        wc = WordCloud(
            font_path=font_path,
            width=width,
            height=height,
            background_color=bg_color,
            max_words=100,
            max_font_size=100,
            random_state=42,
            colormap='viridis'
        )
        
        wc.generate_from_frequencies(word_freq)
        
        return wc.to_image()
        
    except Exception as e:
        print(f"生成词云失败: {e}")
        return None


def create_wordcloud_canvas(parent, word_freq: Dict[str, int],
                             width: int = 600, height: int = 300) -> Optional[tk.Canvas]:
    """
    创建词云画布
    
    Args:
        parent: 父组件
        word_freq: 词频字典
        width: 宽度
        height: 高度
    
    Returns:
        Tkinter Canvas 对象
    """
    canvas = tk.Canvas(parent, width=width, height=height, bg='white')
    
    img = generate_wordcloud(word_freq, width, height)
    
    if img and PIL_AVAILABLE:
        photo = ImageTk.PhotoImage(img)
        canvas.create_image(0, 0, anchor='nw', image=photo)
        canvas.image = photo  # 保持引用
    else:
        canvas.create_text(width//2, height//2, text="词云生成失败\n请检查是否安装了必要的库",
                          font=('Microsoft YaHei', 14), fill='gray')
    
    return canvas


def create_bar_chart(parent, data: List[Tuple[str, int]], 
                      title: str = "热词排行",
                      width: int = 6, height: int = 4) -> Optional[FigureCanvasTkAgg]:
    """
    创建柱状图
    
    Args:
        parent: 父组件
        data: [(word, count), ...]
        title: 图表标题
        width: 图表宽度（英寸）
        height: 图表高度（英寸）
    
    Returns:
        FigureCanvasTkAgg 对象
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    if not data:
        return None
    
    fig = Figure(figsize=(width, height), dpi=100)
    ax = fig.add_subplot(111)
    
    words = [item[0] for item in data[:15]]  # 最多显示15个
    counts = [item[1] for item in data[:15]]
    
    # 水平柱状图
    colors = plt.cm.Blues([(i + 3) / (len(words) + 3) for i in range(len(words))])
    bars = ax.barh(range(len(words)), counts, color=colors)
    
    ax.set_yticks(range(len(words)))
    ax.set_yticklabels(words)
    ax.invert_yaxis()  # 最高的在顶部
    ax.set_xlabel('词频')
    ax.set_title(title)
    
    # 添加数值标签
    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                str(count), va='center', fontsize=9)
    
    fig.tight_layout()
    
    canvas = FigureCanvasTkAgg(fig, parent)
    canvas.draw()  # 确保绘制
    return canvas


def create_sentiment_pie_chart(parent, distribution: Dict,
                                width: int = 5, height: int = 4) -> Optional[FigureCanvasTkAgg]:
    """
    创建情感分布饼图
    
    Args:
        parent: 父组件
        distribution: {'positive': {'count': int, 'ratio': float}, ...}
        width: 图表宽度（英寸）
        height: 图表高度（英寸）
    
    Returns:
        FigureCanvasTkAgg 对象
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    fig = Figure(figsize=(width, height), dpi=100)
    ax = fig.add_subplot(111)
    
    labels = ['正面', '负面', '中性']
    sizes = [
        distribution.get('positive', {}).get('count', 0),
        distribution.get('negative', {}).get('count', 0),
        distribution.get('neutral', {}).get('count', 0)
    ]
    colors = ['#4CAF50', '#F44336', '#9E9E9E']
    explode = (0.05, 0.05, 0)
    
    # 过滤掉0值
    non_zero = [(l, s, c, e) for l, s, c, e in zip(labels, sizes, colors, explode) if s > 0]
    
    if not non_zero:
        ax.text(0.5, 0.5, '暂无数据', ha='center', va='center', fontsize=14)
    else:
        labels, sizes, colors, explode = zip(*non_zero)
        ax.pie(sizes, explode=explode, labels=labels, colors=colors,
               autopct='%1.1f%%', shadow=True, startangle=90)
    
    # ax.set_title('情感分布', pad=20) 
    ax.axis('equal')
    
    # 自动调整布局
    fig.tight_layout()
    
    canvas = FigureCanvasTkAgg(fig, parent)
    canvas.draw()  # 确保绘制
    return canvas


def create_sentiment_bar_chart(parent, word_sentiments: List[Dict],
                                width: int = 8, height: int = 5) -> Optional[FigureCanvasTkAgg]:
    """
    创建热词情感评分柱状图
    
    Args:
        parent: 父组件
        word_sentiments: [{'word': str, 'avg_score': float, ...}, ...]
        width: 图表宽度（英寸）
        height: 图表高度（英寸）
    
    Returns:
        FigureCanvasTkAgg 对象
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    if not word_sentiments:
        return None
    
    fig = Figure(figsize=(width, height), dpi=100)
    ax = fig.add_subplot(111)
    
    words = [item['word'] for item in word_sentiments[:15]]
    scores = [item['avg_score'] for item in word_sentiments[:15]]
    
    # 根据得分设置颜色
    colors = []
    for score in scores:
        if score >= 6.5:
            colors.append('#4CAF50')  # 绿色 - 正面
        elif score <= 4.5:
            colors.append('#F44336')  # 红色 - 负面
        else:
            colors.append('#FF9800')  # 橙色 - 中性
    
    bars = ax.barh(range(len(words)), scores, color=colors)
    
    ax.set_yticks(range(len(words)))
    ax.set_yticklabels(words)
    ax.invert_yaxis()
    ax.set_xlabel('情感评分 (1-10)')
    ax.set_xlim(1, 10)
    ax.axvline(x=5.5, color='gray', linestyle='--', alpha=0.5)
    ax.set_title('热词情感评分')
    
    # 添加数值标签
    for bar, score in zip(bars, scores):
        ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                f'{score:.1f}', va='center', fontsize=9)
    
    fig.tight_layout()
    
    canvas = FigureCanvasTkAgg(fig, parent)
    canvas.draw()  # 确保绘制
    return canvas


def create_trend_chart(parent, date_word_freq: Dict[str, List[Tuple[str, int]]],
                        top_n: int = 5,
                        width: int = 10, height: int = 4) -> Optional[FigureCanvasTkAgg]:
    """
    创建热词趋势图
    
    Args:
        parent: 父组件
        date_word_freq: {date: [(word, count), ...], ...}
        top_n: 显示前N个热词的趋势
        width: 图表宽度（英寸）
        height: 图表高度（英寸）
    
    Returns:
        FigureCanvasTkAgg 对象
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    if not date_word_freq:
        return None
    
    # 获取所有热词
    all_words = {}
    for date, words in date_word_freq.items():
        for word, count in words:
            all_words[word] = all_words.get(word, 0) + count
    
    # 取 Top N 热词
    top_words = sorted(all_words.items(), key=lambda x: x[1], reverse=True)[:top_n]
    top_word_set = {w[0] for w in top_words}
    
    # 准备数据
    dates = sorted(date_word_freq.keys())
    word_trends = {word: [] for word, _ in top_words}
    
    for date in dates:
        word_counts = dict(date_word_freq.get(date, []))
        for word, _ in top_words:
            word_trends[word].append(word_counts.get(word, 0))
    
    # 绘图
    fig = Figure(figsize=(width, height), dpi=100)
    ax = fig.add_subplot(111)
    
    for word, counts in word_trends.items():
        ax.plot(dates, counts, marker='o', label=word, linewidth=2)
    
    ax.set_xlabel('日期')
    ax.set_ylabel('词频')
    ax.set_title('热词趋势')
    ax.legend(loc='upper right')
    
    # 旋转日期标签
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    fig.tight_layout()
    
    canvas = FigureCanvasTkAgg(fig, parent)
    canvas.draw()  # 确保绘制
    return canvas


class ScrollableFrame(ttk.Frame):
    """可滚动的框架"""
    
    def __init__(self, container, *args, **kwargs):
        super().__init__(container, *args, **kwargs)
        
        canvas = tk.Canvas(self)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        
        self.scrollable_frame = ttk.Frame(canvas)
        
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")


class HotWordTable(ttk.Frame):
    """热词表格组件"""
    
    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        
        # 创建表格
        columns = ('rank', 'word', 'count', 'score', 'sentiment')
        self.tree = ttk.Treeview(self, columns=columns, show='headings', height=15)
        
        # 定义列
        self.tree.heading('rank', text='排名')
        self.tree.heading('word', text='热词')
        self.tree.heading('count', text='词频')
        self.tree.heading('score', text='评分')
        self.tree.heading('sentiment', text='情感')
        
        self.tree.column('rank', width=50, anchor='center')
        self.tree.column('word', width=120, anchor='center')
        self.tree.column('count', width=80, anchor='center')
        self.tree.column('score', width=80, anchor='center')
        self.tree.column('sentiment', width=80, anchor='center')
        
        # 滚动条
        scrollbar = ttk.Scrollbar(self, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
    
    def update_data(self, word_sentiments: List[Dict]):
        """更新表格数据"""
        # 清空现有数据
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # 添加新数据
        for i, item in enumerate(word_sentiments, 1):
            sentiment_text = {
                'positive': '😊 正面',
                'negative': '😔 负面',
                'neutral': '😐 中性'
            }.get(item.get('sentiment', 'neutral'), '中性')
            
            self.tree.insert('', 'end', values=(
                i,
                item.get('word', ''),
                item.get('word_count', item.get('total_count', 0)),
                f"{item.get('avg_score', 5.5):.1f}",
                sentiment_text
            ))
