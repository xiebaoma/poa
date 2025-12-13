"""
数据生成器
用于生成模拟的文本数据（微博评论、商品评论、新闻等）
"""
import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path
import sys

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from utils.config_loader import get_data_paths


class DataGenerator:
    """数据生成器类"""
    
    def __init__(self):
        """初始化数据生成器"""
        # 微博评论模板
        self.weibo_templates = [
            "今天天气真好，心情也不错！",
            "这个产品真的很不错，推荐大家购买",
            "服务态度太差了，非常不满意",
            "价格有点贵，但质量还可以",
            "期待已久的新功能终于上线了，太棒了！",
            "系统又崩溃了，什么时候能修复？",
            "用户体验很好，界面设计也很美观",
            "客服回复太慢，等了半天都没人理",
            "功能很强大，值得推荐",
            "bug太多了，严重影响使用",
            "更新后速度变快了，点赞",
            "广告太多，影响正常使用",
            "界面简洁，操作方便",
            "经常闪退，希望尽快修复",
            "性价比很高，值得购买",
            "物流太慢，等了好几天",
            "质量不错，物有所值",
            "售后服务很差，不推荐",
            "功能齐全，满足日常需求",
            "系统不稳定，经常出问题"
        ]
        
        # 商品评论模板
        self.product_templates = [
            "商品质量很好，包装也很精美，非常满意！",
            "物流速度快，商品与描述一致，好评",
            "质量一般，价格偏高，不太推荐",
            "外观设计不错，但功能有待提升",
            "性价比很高，值得购买",
            "收到货后发现有问题，联系客服处理中",
            "使用体验很好，会回购的",
            "商品有瑕疵，但客服态度很好，已解决",
            "功能强大，操作简单，推荐",
            "价格便宜但质量一般，凑合用吧",
            "超出预期，非常满意的一次购物",
            "商品与图片不符，有点失望",
            "发货速度快，商品质量好",
            "售后服务态度差，不推荐购买",
            "性价比不错，适合日常使用",
            "包装简陋，但商品本身还可以",
            "功能齐全，使用方便，满意",
            "质量有问题，退货处理中",
            "外观漂亮，质量也不错",
            "价格实惠，质量对得起价格"
        ]
        
        # 新闻标题模板
        self.news_templates = [
            "科技创新推动经济发展，人工智能应用前景广阔",
            "环保政策出台，绿色能源产业迎来新机遇",
            "教育改革持续推进，学生负担有望减轻",
            "医疗健康领域取得重大突破，新药研发成功",
            "房地产市场调控政策效果显现，价格趋于稳定",
            "就业形势总体稳定，大学生就业率持续提升",
            "食品安全监管加强，消费者权益得到保障",
            "交通基础设施建设加快，出行更加便利",
            "文化产业发展迅速，传统文化焕发新活力",
            "乡村振兴战略实施，农村面貌焕然一新"
        ]
        
        # 数据源类型
        self.sources = ["weibo", "product", "news"]
    
    def generate_text(self, source_type=None):
        """
        生成单条文本数据
        
        Args:
            source_type: 数据源类型（weibo/product/news），如果为None则随机选择
            
        Returns:
            str: 生成的文本内容
        """
        if source_type is None:
            source_type = random.choice(self.sources)
        
        if source_type == "weibo":
            return random.choice(self.weibo_templates)
        elif source_type == "product":
            return random.choice(self.product_templates)
        elif source_type == "news":
            return random.choice(self.news_templates)
        else:
            return random.choice(self.weibo_templates)
    
    def generate_dataset(self, num_records=1000, start_date=None, end_date=None, 
                        source_distribution=None, output_path=None, format='csv'):
        """
        生成数据集
        
        Args:
            num_records: 生成记录数
            start_date: 开始日期（字符串格式：YYYY-MM-DD），如果为None则使用30天前
            end_date: 结束日期（字符串格式：YYYY-MM-DD），如果为None则使用今天
            source_distribution: 数据源分布字典，如 {"weibo": 0.5, "product": 0.3, "news": 0.2}
            output_path: 输出文件路径，如果为None则使用配置的raw_path
            format: 输出格式（csv或json）
            
        Returns:
            pd.DataFrame: 生成的数据集
        """
        # 设置日期范围
        if end_date is None:
            end_date = datetime.now()
        else:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_date is None:
            start_date = end_date - timedelta(days=30)
        else:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        
        # 设置数据源分布
        if source_distribution is None:
            source_distribution = {"weibo": 0.5, "product": 0.3, "news": 0.2}
        
        # 生成数据
        data = []
        for i in range(num_records):
            # 随机选择数据源
            rand = random.random()
            cumulative = 0
            selected_source = "weibo"
            for source, prob in source_distribution.items():
                cumulative += prob
                if rand <= cumulative:
                    selected_source = source
                    break
            
            # 生成随机时间戳
            time_between = end_date - start_date
            days_between = time_between.days
            random_days = random.randrange(days_between)
            random_seconds = random.randrange(86400)  # 一天内的随机秒数
            timestamp = start_date + timedelta(days=random_days, seconds=random_seconds)
            
            # 生成文本内容
            content = self.generate_text(selected_source)
            
            data.append({
                'doc_id': f"doc_{i+1:06d}",
                'content': content,
                'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                'source': selected_source
            })
        
        # 创建DataFrame
        df = pd.DataFrame(data)
        
        # 按时间排序
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        # 保存到文件
        if output_path is None:
            paths = get_data_paths()
            output_path = paths['raw_path'] / f"raw_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{format}"
        else:
            output_path = Path(output_path)
        
        # 确保目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存文件
        if format.lower() == 'csv':
            df.to_csv(output_path, index=False, encoding='utf-8')
        elif format.lower() == 'json':
            df.to_json(output_path, orient='records', force_ascii=False, indent=2)
        else:
            raise ValueError(f"不支持的格式: {format}")
        
        print(f"已生成 {num_records} 条记录，保存到: {output_path}")
        
        return df


def main():
    """主函数，用于测试数据生成器"""
    generator = DataGenerator()
    
    # 生成测试数据
    print("开始生成测试数据...")
    df = generator.generate_dataset(
        num_records=1000,
        start_date="2025-01-01",
        end_date="2025-01-31",
        source_distribution={"weibo": 0.4, "product": 0.4, "news": 0.2}
    )
    
    print(f"\n生成的数据预览:")
    print(df.head(10))
    print(f"\n数据统计:")
    print(df['source'].value_counts())
    print(f"\n时间范围: {df['timestamp'].min()} 到 {df['timestamp'].max()}")


if __name__ == "__main__":
    main()

