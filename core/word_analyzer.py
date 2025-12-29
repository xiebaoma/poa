# -*- coding: utf-8 -*-
"""
热词分析模块
"""
import os
import sys
import re
from typing import List, Dict, Set, Optional
from collections import Counter

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# jieba 分词
try:
    import jieba
    import jieba.analyse
    JIEBA_AVAILABLE = True
except ImportError:
    JIEBA_AVAILABLE = False
    print("警告: jieba 未安装，分词功能不可用")

import pandas as pd


class WordAnalyzer:
    """热词分析器"""
    
    def __init__(self, stopwords_file: str = None):
        """
        初始化分析器
        
        Args:
            stopwords_file: 停用词文件路径
        """
        self.stopwords: Set[str] = set()
        self.min_word_length = config.MIN_WORD_LENGTH
        
        # 加载停用词
        stopwords_file = stopwords_file or config.STOPWORDS_FILE
        self._load_stopwords(stopwords_file)
        
        # 初始化 jieba
        if JIEBA_AVAILABLE:
            jieba.initialize()
    
    def _load_stopwords(self, file_path: str):
        """加载停用词表"""
        if not os.path.exists(file_path):
            print(f"停用词文件不存在: {file_path}")
            return
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    word = line.strip()
                    if word:
                        self.stopwords.add(word)
            print(f"加载停用词 {len(self.stopwords)} 个")
        except Exception as e:
            print(f"加载停用词失败: {e}")
    
    def tokenize(self, text: str) -> List[str]:
        """
        对文本进行分词
        
        Args:
            text: 输入文本
        
        Returns:
            分词结果列表（已过滤停用词和短词）
        """
        if not JIEBA_AVAILABLE:
            # 空格分词
            return [w for w in text.split() if len(w) >= self.min_word_length]
        
        if not text or not isinstance(text, str):
            return []
        
        # 清理文本
        text = self._clean_text(text)
        
        # jieba 精确模式分词
        words = jieba.cut(text, cut_all=False)
        
        # 过滤
        result = []
        for word in words:
            word = word.strip()
            # 过滤条件
            if len(word) < self.min_word_length:
                continue
            if word in self.stopwords:
                continue
            if self._is_invalid_word(word):
                continue
            result.append(word)
        
        return result
    
    def _clean_text(self, text: str) -> str:
        """清理文本"""
        # 移除URL
        text = re.sub(r'http[s]?://\S+', '', text)
        # 移除邮箱
        text = re.sub(r'\S+@\S+', '', text)
        # 移除多余空白
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def _is_invalid_word(self, word: str) -> bool:
        """检查是否为无效词"""
        # 纯数字
        if word.isdigit():
            return True
        # 纯标点
        if re.match(r'^[^\w\u4e00-\u9fff]+$', word):
            return True
        # 单个字母
        if re.match(r'^[a-zA-Z]$', word):
            return True
        return False
    
    def extract_keywords(self, text: str, top_k: int = 10, 
                         method: str = 'tfidf') -> List[tuple]:
        """
        提取关键词
        
        Args:
            text: 输入文本
            top_k: 返回关键词数量
            method: 提取方法 ('tfidf' 或 'textrank')
        
        Returns:
            [(keyword, weight), ...]
        """
        if not JIEBA_AVAILABLE:
            # 后备方案：简单词频
            words = self.tokenize(text)
            counter = Counter(words)
            return counter.most_common(top_k)
        
        if method == 'textrank':
            keywords = jieba.analyse.textrank(text, topK=top_k, withWeight=True)
        else:
            keywords = jieba.analyse.extract_tags(text, topK=top_k, withWeight=True)
        
        return list(keywords)
    
    def analyze_word_frequency(self, texts: List[str]) -> Dict[str, int]:
        """
        分析词频
        
        Args:
            texts: 文本列表
        
        Returns:
            {word: count, ...}
        """
        word_counts = Counter()
        
        for text in texts:
            words = self.tokenize(str(text))
            word_counts.update(words)
        
        return dict(word_counts)
    
    def get_top_words(self, word_freq: Dict[str, int], 
                      top_n: int = None) -> List[tuple]:
        """
        获取 Top N 热词
        
        Args:
            word_freq: 词频字典
            top_n: 返回数量
        
        Returns:
            [(word, count), ...]
        """
        top_n = top_n or config.DEFAULT_TOP_N
        sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        return sorted_words[:top_n]
    
    def analyze_by_date(self, df: pd.DataFrame, 
                        top_n: int = None) -> Dict[str, List[tuple]]:
        """
        按日期分析热词
        
        Args:
            df: 包含 'date' 和 'content' 列的 DataFrame
            top_n: 每日 Top N 热词数量
        
        Returns:
            {date: [(word, count), ...], ...}
        """
        top_n = top_n or config.DEFAULT_TOP_N
        result = {}
        
        if 'date' not in df.columns or 'content' not in df.columns:
            raise ValueError("DataFrame 必须包含 'date' 和 'content' 列")
        
        # 按日期分组
        grouped = df.groupby('date')
        
        for date, group in grouped:
            texts = group['content'].dropna().tolist()
            word_freq = self.analyze_word_frequency(texts)
            top_words = self.get_top_words(word_freq, top_n)
            result[str(date)] = top_words
        
        return result
    
    def get_word_context(self, df: pd.DataFrame, word: str, 
                         max_samples: int = 10) -> List[str]:
        """
        获取包含指定词的文本样本
        
        Args:
            df: DataFrame
            word: 目标词
            max_samples: 最大样本数
        
        Returns:
            文本列表
        """
        if 'content' not in df.columns:
            return []
        
        samples = []
        for content in df['content'].dropna():
            if word in str(content):
                samples.append(str(content))
                if len(samples) >= max_samples:
                    break
        
        return samples


# 全局分析器实例
_analyzer = None

def get_analyzer() -> WordAnalyzer:
    """获取全局词分析器实例"""
    global _analyzer
    if _analyzer is None:
        _analyzer = WordAnalyzer()
    return _analyzer


if __name__ == "__main__":
    # 测试分析器
    print("=" * 50)
    print("测试热词分析器")
    print("=" * 50)
    
    analyzer = get_analyzer()
    
    # 测试分词
    test_texts = [
        "今天天气真好，出去玩吧！",
        "这个产品质量太差了，非常失望",
        "新年快乐，祝大家万事如意！",
        "股市大涨，经济形势一片大好",
    ]
    
    print("\n分词测试:")
    for text in test_texts:
        words = analyzer.tokenize(text)
        print(f"原文: {text}")
        print(f"分词: {words}")
        print()
    
    # 测试词频分析
    print("\n词频分析:")
    word_freq = analyzer.analyze_word_frequency(test_texts)
    top_words = analyzer.get_top_words(word_freq, 10)
    print(f"Top 热词: {top_words}")
    
    # 测试关键词提取
    print("\n关键词提取:")
    long_text = "。".join(test_texts)
    keywords = analyzer.extract_keywords(long_text, top_k=5)
    print(f"关键词: {keywords}")
