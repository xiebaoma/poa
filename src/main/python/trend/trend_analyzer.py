"""
舆情趋势分析器
实现情绪随时间变化分析、负面舆情激增检测等功能
"""
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    date_format, window, when, lit, round as spark_round,
    lag, lead, abs as spark_abs, stddev, percent_rank
)
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType


class TrendAnalyzer:
    """舆情趋势分析器类"""
    
    def __init__(self, spark_session=None, config=None):
        """
        初始化舆情趋势分析器
        
        Args:
            spark_session: SparkSession对象
            config: 配置字典
        """
        if config is None:
            from utils.config_loader import load_config
            config = load_config()
        
        self.config = config
        self.spark = spark_session
        
        # 获取配置参数
        trend_config = config.get('processing', {}).get('trend', {})
        self.time_window = trend_config.get('time_window', 'day')
        self.negative_threshold = trend_config.get('threshold_negative', 0.4)
    
    def _get_time_format(self, time_window=None):
        """根据时间窗口类型获取时间格式"""
        if time_window is None:
            time_window = self.time_window
        
        formats = {
            'hour': 'yyyy-MM-dd HH',
            'day': 'yyyy-MM-dd',
            'week': 'yyyy-ww',
            'month': 'yyyy-MM'
        }
        return formats.get(time_window, 'yyyy-MM-dd')
    
    def sentiment_by_time(self, df, time_column='timestamp', label_column='sentiment_label',
                          time_window=None):
        """
        按时间统计情感分布
        
        Args:
            df: 包含情感标签的DataFrame
            time_column: 时间列名
            label_column: 情感标签列名
            time_window: 时间窗口类型（hour/day/week/month）
            
        Returns:
            DataFrame: 按时间的情感分布统计
        """
        time_format = self._get_time_format(time_window)
        
        # 按时间和情感标签分组统计
        grouped = df.withColumn('time_bucket', date_format(col(time_column), time_format)) \
            .groupBy('time_bucket', label_column) \
            .agg(count('*').alias('count'))
        
        # 透视表：将情感标签转为列
        pivoted = grouped.groupBy('time_bucket').pivot(label_column).sum('count')
        
        # 填充空值为0
        for sentiment in ['positive', 'negative', 'neutral']:
            if sentiment not in pivoted.columns:
                pivoted = pivoted.withColumn(sentiment, lit(0))
            else:
                pivoted = pivoted.withColumn(sentiment, when(col(sentiment).isNull(), 0).otherwise(col(sentiment)))
        
        # 计算总数和百分比
        pivoted = pivoted.withColumn('total', col('positive') + col('negative') + col('neutral'))
        pivoted = pivoted.withColumn('positive_pct', spark_round(col('positive') / col('total') * 100, 2))
        pivoted = pivoted.withColumn('negative_pct', spark_round(col('negative') / col('total') * 100, 2))
        pivoted = pivoted.withColumn('neutral_pct', spark_round(col('neutral') / col('total') * 100, 2))
        
        return pivoted.orderBy('time_bucket')
    
    def sentiment_trend(self, df, time_column='timestamp', score_column='sentiment_score',
                        time_window=None):
        """
        计算情感得分趋势
        
        Args:
            df: 包含情感得分的DataFrame
            time_column: 时间列名
            score_column: 情感得分列名
            time_window: 时间窗口类型
            
        Returns:
            DataFrame: 情感得分趋势
        """
        time_format = self._get_time_format(time_window)
        
        return df.withColumn('time_bucket', date_format(col(time_column), time_format)) \
            .groupBy('time_bucket') \
            .agg(
                count('*').alias('count'),
                avg(score_column).alias('avg_score'),
                spark_min(score_column).alias('min_score'),
                spark_max(score_column).alias('max_score'),
                stddev(score_column).alias('std_score')
            ) \
            .orderBy('time_bucket')
    
    def detect_sentiment_change(self, df, time_column='timestamp', label_column='sentiment_label',
                                 time_window=None, change_threshold=0.1):
        """
        检测情感变化（与前一时间段对比）
        
        Args:
            df: DataFrame
            time_column: 时间列名
            label_column: 情感标签列名
            time_window: 时间窗口类型
            change_threshold: 变化阈值（超过此值视为显著变化）
            
        Returns:
            DataFrame: 包含变化检测结果的DataFrame
        """
        # 获取时间序列情感分布
        trend_df = self.sentiment_by_time(df, time_column, label_column, time_window)
        
        # 使用窗口函数计算与前一时段的变化
        window_spec = Window.orderBy('time_bucket')
        
        trend_df = trend_df \
            .withColumn('prev_positive_pct', lag('positive_pct', 1).over(window_spec)) \
            .withColumn('prev_negative_pct', lag('negative_pct', 1).over(window_spec)) \
            .withColumn('positive_change', col('positive_pct') - col('prev_positive_pct')) \
            .withColumn('negative_change', col('negative_pct') - col('prev_negative_pct'))
        
        # 标记显著变化
        trend_df = trend_df \
            .withColumn('positive_surge', 
                when(col('positive_change') > change_threshold * 100, 'surge')
                .when(col('positive_change') < -change_threshold * 100, 'drop')
                .otherwise('stable')) \
            .withColumn('negative_surge',
                when(col('negative_change') > change_threshold * 100, 'surge')
                .when(col('negative_change') < -change_threshold * 100, 'drop')
                .otherwise('stable'))
        
        return trend_df
    
    def detect_negative_surge(self, df, time_column='timestamp', label_column='sentiment_label',
                              time_window=None, threshold=None):
        """
        检测负面舆情激增
        
        Args:
            df: DataFrame
            time_column: 时间列名
            label_column: 情感标签列名
            time_window: 时间窗口类型
            threshold: 负面比例阈值（超过此值触发预警）
            
        Returns:
            DataFrame: 负面舆情激增的时间段
        """
        if threshold is None:
            threshold = self.negative_threshold
        
        trend_df = self.sentiment_by_time(df, time_column, label_column, time_window)
        
        # 过滤负面比例超过阈值的时间段
        alerts = trend_df.filter(col('negative_pct') >= threshold * 100) \
            .withColumn('alert_level',
                when(col('negative_pct') >= 60, 'critical')
                .when(col('negative_pct') >= 50, 'high')
                .when(col('negative_pct') >= threshold * 100, 'warning')
                .otherwise('normal')) \
            .orderBy(col('negative_pct').desc())
        
        return alerts
    
    def sentiment_by_source_time(self, df, time_column='timestamp', source_column='source',
                                 label_column='sentiment_label', time_window=None):
        """
        按数据源和时间统计情感分布
        
        Args:
            df: DataFrame
            time_column: 时间列名
            source_column: 数据源列名
            label_column: 情感标签列名
            time_window: 时间窗口类型
            
        Returns:
            DataFrame: 按数据源和时间的情感分布
        """
        time_format = self._get_time_format(time_window)
        
        return df.withColumn('time_bucket', date_format(col(time_column), time_format)) \
            .groupBy('time_bucket', source_column, label_column) \
            .agg(count('*').alias('count')) \
            .orderBy('time_bucket', source_column)
    
    def calculate_sentiment_velocity(self, df, time_column='timestamp', score_column='sentiment_score',
                                     time_window=None):
        """
        计算情感变化速度（一阶导数）
        
        Args:
            df: DataFrame
            time_column: 时间列名
            score_column: 情感得分列名
            time_window: 时间窗口类型
            
        Returns:
            DataFrame: 情感变化速度
        """
        trend_df = self.sentiment_trend(df, time_column, score_column, time_window)
        
        window_spec = Window.orderBy('time_bucket')
        
        return trend_df \
            .withColumn('prev_score', lag('avg_score', 1).over(window_spec)) \
            .withColumn('velocity', col('avg_score') - col('prev_score')) \
            .withColumn('velocity_direction',
                when(col('velocity') > 0.05, 'improving')
                .when(col('velocity') < -0.05, 'worsening')
                .otherwise('stable'))
    
    def get_peak_periods(self, df, time_column='timestamp', label_column='sentiment_label',
                         sentiment_type='negative', time_window=None, top_n=5):
        """
        获取情感峰值时段
        
        Args:
            df: DataFrame
            time_column: 时间列名
            label_column: 情感标签列名
            sentiment_type: 情感类型（positive/negative/neutral）
            time_window: 时间窗口类型
            top_n: 返回前N个峰值时段
            
        Returns:
            DataFrame: 峰值时段
        """
        trend_df = self.sentiment_by_time(df, time_column, label_column, time_window)
        
        pct_column = f'{sentiment_type}_pct'
        
        return trend_df.orderBy(col(pct_column).desc()).limit(top_n)
    
    def compare_periods(self, df, period1_start, period1_end, period2_start, period2_end,
                        time_column='timestamp', label_column='sentiment_label'):
        """
        对比两个时间段的情感分布
        
        Args:
            df: DataFrame
            period1_start: 第一时段开始时间
            period1_end: 第一时段结束时间
            period2_start: 第二时段开始时间
            period2_end: 第二时段结束时间
            time_column: 时间列名
            label_column: 情感标签列名
            
        Returns:
            dict: 对比结果
        """
        from pyspark.sql.functions import to_timestamp
        
        # 过滤两个时间段
        df1 = df.filter((col(time_column) >= period1_start) & (col(time_column) <= period1_end))
        df2 = df.filter((col(time_column) >= period2_start) & (col(time_column) <= period2_end))
        
        # 统计各时段情感分布
        def get_distribution(data):
            total = data.count()
            if total == 0:
                return {'positive': 0, 'negative': 0, 'neutral': 0, 'total': 0}
            
            dist = data.groupBy(label_column).count().collect()
            result = {'total': total}
            for row in dist:
                result[row[label_column]] = row['count'] / total * 100
            
            for label in ['positive', 'negative', 'neutral']:
                if label not in result:
                    result[label] = 0
            
            return result
        
        dist1 = get_distribution(df1)
        dist2 = get_distribution(df2)
        
        return {
            'period1': {
                'range': f'{period1_start} ~ {period1_end}',
                'distribution': dist1
            },
            'period2': {
                'range': f'{period2_start} ~ {period2_end}',
                'distribution': dist2
            },
            'change': {
                'positive': dist2.get('positive', 0) - dist1.get('positive', 0),
                'negative': dist2.get('negative', 0) - dist1.get('negative', 0),
                'neutral': dist2.get('neutral', 0) - dist1.get('neutral', 0)
            }
        }
    
    def get_trend_summary(self, df, time_column='timestamp', label_column='sentiment_label',
                          score_column='sentiment_score', time_window=None):
        """
        生成趋势分析摘要
        
        Args:
            df: DataFrame
            time_column: 时间列名
            label_column: 情感标签列名
            score_column: 情感得分列名
            time_window: 时间窗口类型
            
        Returns:
            dict: 趋势分析摘要
        """
        summary = {}
        
        # 整体情感分布
        total = df.count()
        dist = df.groupBy(label_column).count().collect()
        summary['overall_distribution'] = {row[label_column]: row['count'] / total * 100 for row in dist}
        summary['total_records'] = total
        
        # 时间趋势
        trend_df = self.sentiment_by_time(df, time_column, label_column, time_window)
        trend_data = trend_df.collect()
        summary['time_series'] = [
            {
                'time': row['time_bucket'],
                'positive_pct': float(row['positive_pct']),
                'negative_pct': float(row['negative_pct']),
                'neutral_pct': float(row['neutral_pct']),
                'total': int(row['total'])
            }
            for row in trend_data
        ]
        
        # 负面舆情预警
        alerts = self.detect_negative_surge(df, time_column, label_column, time_window)
        alert_data = alerts.collect()
        summary['negative_alerts'] = [
            {
                'time': row['time_bucket'],
                'negative_pct': float(row['negative_pct']),
                'alert_level': row['alert_level']
            }
            for row in alert_data
        ]
        
        # 情感变化速度
        velocity = self.calculate_sentiment_velocity(df, time_column, score_column, time_window)
        velocity_data = velocity.filter(col('velocity').isNotNull()).collect()
        if velocity_data:
            summary['max_improvement'] = max(velocity_data, key=lambda x: x['velocity'] if x['velocity'] else 0)
            summary['max_worsening'] = min(velocity_data, key=lambda x: x['velocity'] if x['velocity'] else 0)
        
        return summary
    
    def print_trend_summary(self, df, time_column='timestamp', label_column='sentiment_label',
                            score_column='sentiment_score', time_window=None):
        """打印趋势分析摘要"""
        summary = self.get_trend_summary(df, time_column, label_column, score_column, time_window)
        
        print("=" * 70)
        print("舆情趋势分析报告")
        print("=" * 70)
        
        print(f"\n【整体情感分布】 (共 {summary['total_records']} 条)")
        for label, pct in summary['overall_distribution'].items():
            print(f"  {label}: {pct:.1f}%")
        
        print(f"\n【时间序列趋势】")
        print(f"{'时间':<15} {'正面%':>10} {'负面%':>10} {'中性%':>10} {'总数':>8}")
        print("-" * 55)
        for item in summary['time_series']:
            print(f"{item['time']:<15} {item['positive_pct']:>10.1f} {item['negative_pct']:>10.1f} "
                  f"{item['neutral_pct']:>10.1f} {item['total']:>8}")
        
        if summary['negative_alerts']:
            print(f"\n【负面舆情预警】")
            for alert in summary['negative_alerts']:
                print(f"  ⚠️ {alert['time']}: 负面 {alert['negative_pct']:.1f}% [{alert['alert_level']}]")
        else:
            print(f"\n【负面舆情预警】")
            print("  ✓ 暂无预警")
        
        print("=" * 70)

