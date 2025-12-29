# -*- coding: utf-8 -*-
"""
主窗口模块

Python+Spark 热词分析系统 GUI 界面
"""
import os
import sys
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# GUI 库
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

try:
    import ttkbootstrap as ttkb
    from ttkbootstrap.constants import *
    TTKBOOTSTRAP_AVAILABLE = True
except ImportError:
    TTKBOOTSTRAP_AVAILABLE = False
    print("提示: ttkbootstrap 未安装")

import pandas as pd

# 导入核心模块
from core import get_analyzer, get_sentiment_analyzer, get_processor
from utils import load_covid_weibo_data, get_date_range, filter_by_date
from gui.components import (
    generate_wordcloud, create_bar_chart, create_sentiment_pie_chart,
    create_sentiment_bar_chart, HotWordTable
)

try:
    from PIL import Image, ImageTk
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False


class MainWindow:
    """主窗口类"""
    
    def __init__(self):
        """初始化主窗口"""
        # 创建主窗口
        if TTKBOOTSTRAP_AVAILABLE:
            self.root = ttkb.Window(themename="cosmo")
        else:
            self.root = tk.Tk()
        
        self.root.title(config.WINDOW_TITLE)
        self.root.geometry(f"{config.WINDOW_WIDTH}x{config.WINDOW_HEIGHT}")
        
        # 数据
        self.df: Optional[pd.DataFrame] = None
        self.word_freq: Dict[str, int] = {}
        self.top_words: List[Tuple[str, int]] = []
        self.word_sentiments: List[Dict] = []
        self.date_word_freq: Dict[str, List[Tuple[str, int]]] = {}
        
        # 分析器
        self.word_analyzer = get_analyzer()
        self.sentiment_analyzer = get_sentiment_analyzer()
        self.spark_processor = get_processor()
        
        # 状态
        self.is_analyzing = False
        
        # 创建UI
        self._create_ui()
        
        # 绑定关闭事件
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
    
    def _create_ui(self):
        """创建用户界面"""
        # 主框架
        main_frame = ttk.Frame(self.root, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        # 顶部控制区
        self._create_control_panel(main_frame)
        
        # 中间内容区
        content_frame = ttk.Frame(main_frame)
        content_frame.pack(fill='both', expand=True, pady=10)
        
        # 右侧 - 热词表格和情感分析 (先Pack右侧，确保固定宽度不被挤占)
        right_frame = ttk.Frame(content_frame, width=450)
        right_frame.pack(side='right', fill='both', padx=(5, 0))
        right_frame.pack_propagate(False)
        
        # 左侧 - 词云和图表 (后Pack左侧，占据剩余空间)
        left_frame = ttk.Frame(content_frame)
        left_frame.pack(side='left', fill='both', expand=True, padx=(0, 5))
        
        self._create_wordcloud_panel(left_frame)
        self._create_chart_panel(left_frame)
        
        self._create_hotword_panel(right_frame)
        self._create_sentiment_panel(right_frame)
        
        # 底部状态栏
        self._create_status_bar(main_frame)
    
    def _create_control_panel(self, parent):
        """创建控制面板"""
        control_frame = ttk.LabelFrame(parent, text="控制面板", padding=10)
        control_frame.pack(fill='x', pady=(0, 10))
        
        # 第一行 - 文件操作
        row1 = ttk.Frame(control_frame)
        row1.pack(fill='x', pady=5)
        
        if TTKBOOTSTRAP_AVAILABLE:
            ttk.Button(row1, text="📊 加载数据", command=self._load_data,
                      bootstyle="primary").pack(side='left', padx=5)
            self.analyze_btn = ttk.Button(row1, text="🔍 开始分析", command=self._start_analysis,
                                          bootstyle="success")
            self.analyze_btn.pack(side='left', padx=5)
            ttk.Button(row1, text="💾 导出结果", command=self._export_results,
                      bootstyle="secondary").pack(side='left', padx=5)
        else:
            ttk.Button(row1, text="加载数据", command=self._load_data).pack(side='left', padx=5)
            self.analyze_btn = ttk.Button(row1, text="开始分析", command=self._start_analysis)
            self.analyze_btn.pack(side='left', padx=5)
            ttk.Button(row1, text="导出结果", command=self._export_results).pack(side='left', padx=5)
        
        # 数据信息标签
        self.data_info_label = ttk.Label(row1, text="未加载数据")
        self.data_info_label.pack(side='right', padx=10)
        
        # 第二行 - 日期和参数
        row2 = ttk.Frame(control_frame)
        row2.pack(fill='x', pady=5)
        
        ttk.Label(row2, text="日期范围:").pack(side='left', padx=5)
        
        self.start_date_var = tk.StringVar()
        self.start_date_entry = ttk.Entry(row2, textvariable=self.start_date_var, width=12)
        self.start_date_entry.pack(side='left', padx=2)
        
        ttk.Label(row2, text="至").pack(side='left', padx=5)
        
        self.end_date_var = tk.StringVar()
        self.end_date_entry = ttk.Entry(row2, textvariable=self.end_date_var, width=12)
        self.end_date_entry.pack(side='left', padx=2)
        
        ttk.Label(row2, text="Top N:").pack(side='left', padx=(20, 5))
        
        self.top_n_var = tk.IntVar(value=config.DEFAULT_TOP_N)
        top_n_spinbox = ttk.Spinbox(row2, from_=5, to=50, textvariable=self.top_n_var, width=5)
        top_n_spinbox.pack(side='left', padx=2)
        
        # 使用Spark选项
        self.use_spark_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(row2, text="使用 Spark", variable=self.use_spark_var).pack(side='left', padx=20)
    
    def _create_wordcloud_panel(self, parent):
        """创建词云面板"""
        wc_frame = ttk.LabelFrame(parent, text="词云可视化", padding=10)
        wc_frame.pack(fill='both', expand=True, pady=(0, 5))
        
        self.wordcloud_canvas = tk.Canvas(wc_frame, bg='white', height=300)
        self.wordcloud_canvas.pack(fill='both', expand=True)
        
        # 显示占位文字
        self.wordcloud_canvas.create_text(
            300, 150, text="请导入数据并点击\"开始分析\"",
            font=('Microsoft YaHei', 14), fill='gray'
        )
    
    def _create_chart_panel(self, parent):
        """创建图表面板"""
        chart_frame = ttk.LabelFrame(parent, text="统计图表", padding=10)
        chart_frame.pack(fill='both', expand=True, pady=(5, 0))
        
        # 使用 Notebook 切换不同图表
        self.chart_notebook = ttk.Notebook(chart_frame)
        self.chart_notebook.pack(fill='both', expand=True)
        
        # 柱状图标签页
        self.bar_chart_frame = ttk.Frame(self.chart_notebook)
        self.chart_notebook.add(self.bar_chart_frame, text="热词排行")
        
        # 情感评分标签页
        self.sentiment_chart_frame = ttk.Frame(self.chart_notebook)
        self.chart_notebook.add(self.sentiment_chart_frame, text="情感评分")
        

        self.chart_notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)
    
    def _on_tab_changed(self, event):
        """Tab切换时刷新图表"""

        current_tab = self.chart_notebook.index(self.chart_notebook.select())
        if current_tab == 0:  # 热词排行
            if hasattr(self.bar_chart_frame, '_chart'):
                try:
                    self.bar_chart_frame._chart.draw()
                except:
                    pass
        elif current_tab == 1:  # 情感评分
            if hasattr(self.sentiment_chart_frame, '_chart'):
                try:
                    self.sentiment_chart_frame._chart.draw()
                except:
                    pass
    
    def _create_hotword_panel(self, parent):
        """创建热词表格面板"""
        hw_frame = ttk.LabelFrame(parent, text="热词排行榜", padding=10)
        hw_frame.pack(fill='both', expand=True, pady=(0, 5))  # 热词表占据主要空间
        
        self.hotword_table = HotWordTable(hw_frame)
        self.hotword_table.pack(fill='both', expand=True)
    
    def _create_sentiment_panel(self, parent):
        """创建情感分析面板"""
        st_frame = ttk.LabelFrame(parent, text="情感分布", padding=10, height=300)
        st_frame.pack(fill='both', expand=False, pady=(5, 0))
        st_frame.pack_propagate(False) # 强制固定高度
        
        self.sentiment_chart_container = ttk.Frame(st_frame)
        self.sentiment_chart_container.pack(fill='both', expand=True)
        
        # 占位标签
        self.sentiment_placeholder = ttk.Label(
            self.sentiment_chart_container, 
            text="等待分析...",
            font=('Microsoft YaHei', 12)
        )
        self.sentiment_placeholder.pack(expand=True)
    
    def _create_status_bar(self, parent):
        """创建状态栏"""
        status_frame = ttk.Frame(parent)
        status_frame.pack(fill='x', pady=(10, 0))
        
        self.status_var = tk.StringVar(value="就绪")
        status_label = ttk.Label(status_frame, textvariable=self.status_var)
        status_label.pack(side='left')
        
        # 进度条
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(
            status_frame, variable=self.progress_var,
            maximum=100, length=200
        )
        self.progress_bar.pack(side='right', padx=10)
    
    def _load_data(self):
        """加载本地 COVID-19 微博情感数据集"""
        try:
            self._set_status("正在加载数据...")
            self.progress_var.set(10)
            self.root.update()
            
            # 加载本地 COVID-19 微博情感数据
            self.df = load_covid_weibo_data()
            
            if self.df is not None:
                self._on_data_loaded()
                messagebox.showinfo("成功", 
                    f"数据加载完成!\n"
                    f"共 {len(self.df)} 条记录\n"
                    f"日期范围: 2020-01-20 ~ 2020-04-30")
            else:
                messagebox.showerror("错误", "加载数据失败")
                self._set_status("加载失败")
                
        except Exception as e:
            messagebox.showerror("错误", f"加载失败:\n{e}")
            self._set_status("加载失败")
        finally:
            self.progress_var.set(0)
    
    def _on_data_loaded(self):
        """数据加载完成后的处理"""
        if self.df is None:
            return
        
        # 更新数据信息
        min_date, max_date = get_date_range(self.df)
        
        info_text = f"已加载 {len(self.df)} 条数据"
        if min_date and max_date:
            self.start_date_var.set(min_date.strftime('%Y-%m-%d'))
            self.end_date_var.set(max_date.strftime('%Y-%m-%d'))
            info_text += f" | 日期: {min_date.strftime('%Y-%m-%d')} ~ {max_date.strftime('%Y-%m-%d')}"
        
        self.data_info_label.config(text=info_text)
        self._set_status("数据加载完成")
        self.progress_var.set(0)
    
    def _start_analysis(self):
        """开始分析"""
        if self.df is None:
            messagebox.showwarning("提示", "请先导入数据")
            return
        
        if self.is_analyzing:
            return
        
        self.is_analyzing = True
        self.analyze_btn.config(state='disabled')
        
        # 在线程中执行分析
        thread = threading.Thread(target=self._run_analysis)
        thread.start()
    
    def _run_analysis(self):
        """执行分析（在线程中运行）"""
        try:
            self._set_status("正在分析...")
            self.progress_var.set(10)
            
            # 筛选日期范围
            start_date = self.start_date_var.get()
            end_date = self.end_date_var.get()
            
            # 验证日期格式
            try:
                if start_date:
                    pd.to_datetime(start_date)
                if end_date:
                    pd.to_datetime(end_date)
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("日期错误", f"日期格式无效: {e}\n请使用 YYYY-MM-DD 格式"))
                return
            
            df = filter_by_date(self.df, start_date, end_date)
            
            if len(df) == 0:
                self.root.after(0, lambda: messagebox.showwarning("提示", "所选日期范围内没有数据"))
                return
            
            self._set_status(f"分析 {len(df)} 条数据...")
            self.progress_var.set(20)
            
            top_n = self.top_n_var.get()
            
            # 词频分析
            if self.use_spark_var.get():
                self._set_status("使用 Spark 进行词频分析...")
                word_freq_df = self.spark_processor.process_word_frequency(
                    df, self.word_analyzer.tokenize, group_by_date=True
                )
                # 提取总体词频
                self.word_freq = word_freq_df.groupby('word')['count'].sum().to_dict()
            else:
                self._set_status("使用 Pandas 进行词频分析...")
                texts = df['content'].dropna().tolist()
                self.word_freq = self.word_analyzer.analyze_word_frequency(texts)
            
            self.progress_var.set(50)
            
            # 获取 Top N 热词
            self.top_words = self.word_analyzer.get_top_words(self.word_freq, top_n)
            
            self._set_status("进行情感分析...")
            self.progress_var.set(60)
            
            # 情感分析
            self.word_sentiments = self.sentiment_analyzer.analyze_top_words_sentiment(
                df, self.top_words
            )
            
            self.progress_var.set(80)
            
            # 获取每日热词
            self._set_status("统计每日热词...")
            self.date_word_freq = self.word_analyzer.analyze_by_date(df, top_n)
            
            # 获取情感分布
            self.sentiment_distribution = self.sentiment_analyzer.get_sentiment_distribution(df)
            
            self.progress_var.set(100)
            
            # 在主线程更新UI
            self.root.after(0, self._update_ui)
            
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("错误", f"分析失败:\n{e}"))
            import traceback
            traceback.print_exc()
        finally:
            self.is_analyzing = False
            self.root.after(0, lambda: self.analyze_btn.config(state='normal'))
            self.root.after(0, lambda: self._set_status("分析完成"))
    
    def _update_ui(self):
        """更新UI显示"""
        # 更新词云
        self._update_wordcloud()
        
        # 更新热词表格
        self.hotword_table.update_data(self.word_sentiments)
        
        # 更新图表
        self._update_charts()
        
        # 更新情感分布
        self._update_sentiment_chart()
        
        self.progress_var.set(0)
    
    def _update_wordcloud(self):
        """更新词云"""
        self.wordcloud_canvas.delete('all')
        
        if not self.word_freq:
            self.wordcloud_canvas.create_text(
                300, 150, text="暂无数据",
                font=('Microsoft YaHei', 14), fill='gray'
            )
            return
        
        # 生成词云
        try:
            from gui.components import generate_wordcloud
            
            # 获取画布大小
            self.root.update()
            width = self.wordcloud_canvas.winfo_width()
            height = self.wordcloud_canvas.winfo_height()
            
            if width < 100:
                width = 600
            if height < 100:
                height = 300
            
            img = generate_wordcloud(self.word_freq, width, height)
            
            if img and PIL_AVAILABLE:
                photo = ImageTk.PhotoImage(img)
                self.wordcloud_canvas.create_image(0, 0, anchor='nw', image=photo)
                self.wordcloud_canvas.image = photo
            else:
                self.wordcloud_canvas.create_text(
                    width//2, height//2, text="词云生成失败",
                    font=('Microsoft YaHei', 14), fill='gray'
                )
        except Exception as e:
            print(f"生成词云失败: {e}")
            self.wordcloud_canvas.create_text(
                300, 150, text=f"词云生成失败: {e}",
                font=('Microsoft YaHei', 12), fill='red'
            )
    
    def _update_charts(self):
        """更新图表"""
        # 清空现有图表
        for widget in self.bar_chart_frame.winfo_children():
            widget.destroy()
        for widget in self.sentiment_chart_frame.winfo_children():
            widget.destroy()
        
        # 热词柱状图
        if self.top_words:
            chart = create_bar_chart(self.bar_chart_frame, self.top_words, "热词排行")
            if chart:
                widget = chart.get_tk_widget()
                widget.pack(fill='both', expand=True)
             
                self.bar_chart_frame._chart = chart
                self.bar_chart_frame._chart_widget = widget
        
        # 情感评分柱状图
        if self.word_sentiments:
            chart = create_sentiment_bar_chart(self.sentiment_chart_frame, self.word_sentiments)
            if chart:
                widget = chart.get_tk_widget()
                widget.pack(fill='both', expand=True)
            
                self.sentiment_chart_frame._chart = chart
                self.sentiment_chart_frame._chart_widget = widget
    
    def _update_sentiment_chart(self):
        """更新情感分布图"""
        # 清空现有内容
        for widget in self.sentiment_chart_container.winfo_children():
            widget.destroy()
        
        if hasattr(self, 'sentiment_distribution'):
            chart = create_sentiment_pie_chart(
                self.sentiment_chart_container,
                self.sentiment_distribution
            )
            if chart:
                widget = chart.get_tk_widget()
                widget.pack(fill='both', expand=True)
             
                self.sentiment_chart_container._chart = chart
                self.sentiment_chart_container._chart_widget = widget
            
            # 添加文字说明
            dist = self.sentiment_distribution
            info_text = f"平均评分: {dist.get('avg_score', 5.5):.1f}/10"
            ttk.Label(
                self.sentiment_chart_container,
                text=info_text,
                font=('Microsoft YaHei', 11)
            ).pack(pady=5)
    
    def _export_results(self):
        """导出结果"""
        if not self.word_sentiments:
            messagebox.showwarning("提示", "请先进行分析")
            return
        
        file_path = filedialog.asksaveasfilename(
            title="导出结果",
            defaultextension=".csv",
            filetypes=[
                ("CSV 文件", "*.csv"),
                ("所有文件", "*.*")
            ]
        )
        
        if not file_path:
            return
        
        try:
            # 创建导出数据
            export_data = []
            for i, item in enumerate(self.word_sentiments, 1):
                export_data.append({
                    '排名': i,
                    '热词': item.get('word', ''),
                    '词频': item.get('word_count', item.get('total_count', 0)),
                    '情感评分': item.get('avg_score', 5.5),
                    '情感倾向': item.get('sentiment', 'neutral'),
                    '正面比例': item.get('positive_ratio', 0),
                    '负面比例': item.get('negative_ratio', 0)
                })
            
            df = pd.DataFrame(export_data)
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            messagebox.showinfo("成功", f"结果已导出到:\n{file_path}")
            
        except Exception as e:
            messagebox.showerror("错误", f"导出失败:\n{e}")
    
    def _set_status(self, text: str):
        """设置状态栏文字"""
        self.status_var.set(text)
        self.root.update()
    
    def _on_close(self):
        """关闭窗口"""
        # 停止 Spark
        try:
            self.spark_processor.stop()
        except:
            pass
        
        self.root.destroy()
    
    def run(self):
        """运行主窗口"""
        self.root.mainloop()


def main():
    """主函数"""
    app = MainWindow()
    app.run()


if __name__ == "__main__":
    main()
