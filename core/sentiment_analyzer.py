# -*- coding: utf-8 -*-
"""
情感分析模块
"""
import os
import sys
from typing import List, Dict, Tuple, Optional
import pandas as pd

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# SnowNLP 情感分析
try:
    from snownlp import SnowNLP
    SNOWNLP_AVAILABLE = True
except ImportError:
    SNOWNLP_AVAILABLE = False
    print("警告: SnowNLP 未安装，情感分析功能受限")


class SentimentAnalyzer:
    """情感分析器"""
    
    def __init__(self):
        """初始化分析器"""
        self.positive_threshold = config.POSITIVE_THRESHOLD
        self.negative_threshold = config.NEGATIVE_THRESHOLD
    
    def analyze(self, text: str) -> Dict:
        """
        分析单条文本的情感
        
        Args:
            text: 输入文本
        
        Returns:
            {
                'raw_score': float,  # 原始得分 (0-1)
                'score': float,      # 映射得分 (1-10)
                'sentiment': str,    # 情感倾向 ('positive', 'negative', 'neutral')
                'label': int        # 情感标签 (1正面, 0负面, -1中性)
            }
        """
        if not text or not isinstance(text, str) or len(text.strip()) == 0:
            return {
                'raw_score': 0.5,
                'score': 5.5,
                'sentiment': 'neutral',
                'label': -1
            }
        
        try:
            if SNOWNLP_AVAILABLE:
                s = SnowNLP(text)
                raw_score = s.sentiments  # 0-1 之间
            else:
                # 后备方案：基于关键词的简单分析
                raw_score = self._simple_sentiment(text)
            
            # 映射到 1-10 分
            score = raw_score * 9 + 1
            
            # 判断情感倾向
            if raw_score >= self.positive_threshold:
                sentiment = 'positive'
                label = 1
            elif raw_score <= self.negative_threshold:
                sentiment = 'negative'
                label = 0
            else:
                sentiment = 'neutral'
                label = -1
            
            return {
                'raw_score': round(raw_score, 4),
                'score': round(score, 2),
                'sentiment': sentiment,
                'label': label
            }
            
        except Exception as e:
            print(f"情感分析错误: {e}")
            return {
                'raw_score': 0.5,
                'score': 5.5,
                'sentiment': 'neutral',
                'label': -1
            }
    
    def _simple_sentiment(self, text: str) -> float:
        """简单的基于关键词的情感分析（后备方案）"""
        positive_words = {'好', '棒', '赞', '喜欢', '开心', '快乐', '优秀', '完美', 
                         '感谢', '推荐', '满意', '精彩', '厉害', '漂亮', '美丽'}
        negative_words = {'差', '烂', '坏', '讨厌', '失望', '难过', '垃圾', '糟糕',
                         '恶心', '愤怒', '无聊', '可怕', '糟', '骗', '假'}
        
        pos_count = sum(1 for word in positive_words if word in text)
        neg_count = sum(1 for word in negative_words if word in text)
        
        total = pos_count + neg_count
        if total == 0:
            return 0.5
        
        return pos_count / total
    
    def analyze_batch(self, texts: List[str]) -> List[Dict]:
        """
        批量分析文本情感
        
        Args:
            texts: 文本列表
        
        Returns:
            分析结果列表
        """
        return [self.analyze(text) for text in texts]
    
    def analyze_dataframe(self, df: pd.DataFrame, 
                          content_col: str = 'content') -> pd.DataFrame:
        """
        对 DataFrame 中的文本进行情感分析
        
        Args:
            df: DataFrame
            content_col: 文本列名
        
        Returns:
            添加情感分析结果的 DataFrame
        """
        df = df.copy()
        
        results = self.analyze_batch(df[content_col].fillna('').tolist())
        
        df['sentiment_raw_score'] = [r['raw_score'] for r in results]
        df['sentiment_score'] = [r['score'] for r in results]
        df['sentiment'] = [r['sentiment'] for r in results]
        df['sentiment_label'] = [r['label'] for r in results]
        
        return df
    
    def analyze_word_sentiment(self, df: pd.DataFrame, word: str,
                                content_col: str = 'content',
                                max_samples: int = 50) -> Dict:
        """
        分析包含特定词的文本的情感（采样加速）
        
        Args:
            df: DataFrame
            word: 目标热词
            content_col: 文本列名
            max_samples: 最大采样数（加速用）
        
        Returns:
            情感分析结果字典
        """
        # 筛选包含该词的文本
        mask = df[content_col].fillna('').str.contains(word, na=False)
        filtered_df = df[mask]
        
        total_count = len(filtered_df)
        
        if total_count == 0:
            return {
                'word': word,
                'total_count': 0,
                'avg_score': 5.5,
                'sentiment': 'neutral',
                'positive_count': 0,
                'negative_count': 0,
                'neutral_count': 0,
                'positive_ratio': 0,
                'negative_ratio': 0
            }
        
        # 采样分析（加速）
        if total_count > max_samples:
            sample_df = filtered_df.sample(n=max_samples, random_state=42)
        else:
            sample_df = filtered_df
        
        # 分析情感
        results = self.analyze_batch(sample_df[content_col].tolist())
        
        scores = [r['score'] for r in results]
        sentiments = [r['sentiment'] for r in results]
        
        sample_size = len(results)
        positive_count = sentiments.count('positive')
        negative_count = sentiments.count('negative')
        neutral_count = sentiments.count('neutral')
        
        avg_score = sum(scores) / len(scores) if scores else 5.5
        
        # 判断整体情感倾向
        if positive_count > negative_count:
            overall_sentiment = 'positive'
        elif negative_count > positive_count:
            overall_sentiment = 'negative'
        else:
            overall_sentiment = 'neutral'
        
        return {
            'word': word,
            'total_count': total_count,  # 实际包含该词的文本数
            'avg_score': round(avg_score, 2),
            'sentiment': overall_sentiment,
            'positive_count': positive_count,
            'negative_count': negative_count,
            'neutral_count': neutral_count,
            'positive_ratio': round(positive_count / sample_size, 4) if sample_size > 0 else 0,
            'negative_ratio': round(negative_count / sample_size, 4) if sample_size > 0 else 0
        }
    
    def analyze_top_words_sentiment(self, df: pd.DataFrame,
                                     top_words: List[Tuple[str, int]],
                                     content_col: str = 'content') -> List[Dict]:
        """
        分析 Top 热词的情感
        
        Args:
            df: DataFrame
            top_words: [(word, count), ...]
            content_col: 文本列名
        
        Returns:
            情感分析结果列表
        """
        results = []
        
        for word, count in top_words:
            result = self.analyze_word_sentiment(df, word, content_col)
            result['word_count'] = count  # 添加词频
            results.append(result)
        
        return results
    
    def get_sentiment_distribution(self, df: pd.DataFrame,
                                    content_col: str = 'content') -> Dict:
        """
        获取情感分布
        
        Args:
            df: DataFrame
            content_col: 文本列名
        
        Returns:
            {
                'positive': {'count': int, 'ratio': float},
                'negative': {'count': int, 'ratio': float},
                'neutral': {'count': int, 'ratio': float},
                'avg_score': float
            }
        """
        results = self.analyze_batch(df[content_col].fillna('').tolist())
        
        total = len(results)
        if total == 0:
            return {
                'positive': {'count': 0, 'ratio': 0},
                'negative': {'count': 0, 'ratio': 0},
                'neutral': {'count': 0, 'ratio': 0},
                'avg_score': 5.5
            }
        
        sentiments = [r['sentiment'] for r in results]
        scores = [r['score'] for r in results]
        
        positive_count = sentiments.count('positive')
        negative_count = sentiments.count('negative')
        neutral_count = sentiments.count('neutral')
        
        return {
            'positive': {
                'count': positive_count,
                'ratio': round(positive_count / total, 4)
            },
            'negative': {
                'count': negative_count,
                'ratio': round(negative_count / total, 4)
            },
            'neutral': {
                'count': neutral_count,
                'ratio': round(neutral_count / total, 4)
            },
            'avg_score': round(sum(scores) / len(scores), 2)
        }
    
    def analyze_word_sentiment_by_date(self, df: pd.DataFrame, word: str,
                                        content_col: str = 'content',
                                        date_col: str = 'date',
                                        max_samples_per_day: int = 20) -> Dict[str, float]:
        """
        分析特定热词在每天的情感评分
        
        Args:
            df: DataFrame
            word: 目标热词
            content_col: 文本列名
            date_col: 日期列名
            max_samples_per_day: 每天最大采样数（加速用）
        
        Returns:
            {date_str: avg_score, ...}  日期到平均情感评分的字典
        """
        # 筛选包含该词的文本
        mask = df[content_col].fillna('').str.contains(word, na=False)
        filtered_df = df[mask].copy()
        
        if len(filtered_df) == 0:
            return {}
        
        # 转换日期列
        filtered_df['_date'] = pd.to_datetime(filtered_df[date_col], errors='coerce')
        filtered_df = filtered_df.dropna(subset=['_date'])
        
        if len(filtered_df) == 0:
            return {}
        
        # 提取日期（去掉时间部分）
        filtered_df['_date_str'] = filtered_df['_date'].dt.strftime('%Y-%m-%d')
        
        # 按日期分组
        date_scores = {}
        
        for date_str, group_df in filtered_df.groupby('_date_str'):
            # 采样（加速）
            if len(group_df) > max_samples_per_day:
                sample_df = group_df.sample(n=max_samples_per_day, random_state=42)
            else:
                sample_df = group_df
            
            # 分析情感
            results = self.analyze_batch(sample_df[content_col].tolist())
            scores = [r['score'] for r in results]
            
            if scores:
                avg_score = sum(scores) / len(scores)
                date_scores[date_str] = round(avg_score, 2)
        
        return date_scores


# 全局分析器实例
_analyzer = None

def get_sentiment_analyzer() -> SentimentAnalyzer:
    """获取全局情感分析器实例"""
    global _analyzer
    if _analyzer is None:
        _analyzer = SentimentAnalyzer()
    return _analyzer


if __name__ == "__main__":
    # 测试情感分析器
    print("=" * 50)
    print("测试情感分析器")
    print("=" * 50)
    
    analyzer = get_sentiment_analyzer()
    
    # 测试文本
    test_texts = [
        "这个产品太好用了！强烈推荐给大家！",
        "服务态度很差，非常失望，再也不会购买了",
        "今天天气还可以，没什么特别的",
        "新年快乐，祝大家万事如意，心想事成！",
        "真是太生气了，简直是骗子公司！",
    ]
    
    print("\n情感分析测试:")
    for text in test_texts:
        result = analyzer.analyze(text)
        print(f"文本: {text[:30]}...")
        print(f"  得分: {result['score']}/10, 情感: {result['sentiment']}")
        print()
    
    # 测试 DataFrame 分析
    print("\nDataFrame 情感分析测试:")
    df = pd.DataFrame({
        'content': test_texts
    })
    
    result_df = analyzer.analyze_dataframe(df)
    print(result_df[['content', 'sentiment_score', 'sentiment']])
    
    # 测试情感分布
    print("\n情感分布:")
    distribution = analyzer.get_sentiment_distribution(df)
    print(f"正面: {distribution['positive']['ratio']*100:.1f}%")
    print(f"负面: {distribution['negative']['ratio']*100:.1f}%")
    print(f"中性: {distribution['neutral']['ratio']*100:.1f}%")
    print(f"平均得分: {distribution['avg_score']}/10")
