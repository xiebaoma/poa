"""
舆情分析系统 - Streamlit 可视化界面
支持数据加载、ETL预处理、情感分析、话题挖掘、趋势分析的完整可视化展示
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
from datetime import datetime, timedelta

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src/main/python"))

from main.app import SentimentAnalysisApp


# 页面配置
st.set_page_config(
    page_title="舆情分析系统",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义样式
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #ff7f0e;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid #ff7f0e;
        padding-bottom: 0.5rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
    }
    .stAlert {
        margin-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)


def init_session_state():
    """初始化会话状态"""
    if 'app' not in st.session_state:
        st.session_state.app = None
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False


def render_sidebar():
    """渲染侧边栏配置"""
    st.sidebar.markdown("## ⚙️ 系统配置")
    
    # 数据源选择
    st.sidebar.markdown("### 1️⃣ 数据源")
    data_source = st.sidebar.radio(
        "选择数据源",
        ["生成模拟数据", "加载已有数据"],
        help="选择使用模拟数据还是加载已有数据文件"
    )
    
    if data_source == "生成模拟数据":
        num_records = st.sidebar.slider(
            "生成记录数",
            min_value=100,
            max_value=10000,
            value=1000,
            step=100,
            help="生成的模拟数据记录数量"
        )
        
        date_range = st.sidebar.date_input(
            "日期范围",
            value=(
                datetime.now() - timedelta(days=30),
                datetime.now()
            ),
            help="生成数据的日期范围"
        )
        
        config = {
            'data_source': 'generate',
            'num_records': num_records,
            'start_date': str(date_range[0]) if len(date_range) > 0 else None,
            'end_date': str(date_range[1]) if len(date_range) > 1 else None
        }
    else:
        input_path = st.sidebar.text_input(
            "数据路径",
            value="data/raw",
            help="输入数据文件或目录路径"
        )
        
        file_format = st.sidebar.selectbox(
            "文件格式",
            ["csv", "json", "parquet"],
            help="数据文件格式"
        )
        
        config = {
            'data_source': 'load',
            'input_path': input_path,
            'file_format': file_format
        }
    
    # 分析参数
    st.sidebar.markdown("### 2️⃣ 分析参数")
    
    top_n = st.sidebar.slider(
        "Top-N 关键词数量",
        min_value=5,
        max_value=50,
        value=20,
        step=5,
        help="展示的热点关键词数量"
    )
    
    time_window = st.sidebar.selectbox(
        "时间窗口",
        ["hour", "day", "week", "month"],
        index=1,
        help="趋势分析的时间聚合粒度"
    )
    
    config['top_n'] = top_n
    config['time_window'] = time_window
    
    # 运行按钮
    st.sidebar.markdown("---")
    run_analysis = st.sidebar.button(
        "🚀 运行分析",
        type="primary",
        use_container_width=True
    )
    
    # 清除结果按钮
    if st.session_state.analysis_results is not None:
        if st.sidebar.button("🗑️ 清除结果", use_container_width=True):
            st.session_state.analysis_results = None
            st.session_state.data_loaded = False
            if st.session_state.app:
                st.session_state.app.stop()
                st.session_state.app = None
            st.rerun()
    
    return run_analysis, config


def run_analysis_pipeline(config):
    """运行分析流水线"""
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # 初始化应用
        status_text.text("初始化系统...")
        progress_bar.progress(10)
        
        if st.session_state.app is None:
            st.session_state.app = SentimentAnalysisApp()
        app = st.session_state.app
        
        # 加载/生成数据
        status_text.text("加载数据...")
        progress_bar.progress(20)
        
        if config['data_source'] == 'generate':
            app.generate_data(
                num_records=config['num_records'],
                start_date=config.get('start_date'),
                end_date=config.get('end_date')
            )
            df = app.load_data()
        else:
            df = app.load_data(
                file_path=config.get('input_path'),
                file_format=config.get('file_format', 'csv')
            )
        
        # ETL预处理
        status_text.text("数据预处理中...")
        progress_bar.progress(40)
        df_processed = app.preprocess(df)
        df_processed = df_processed.cache()
        
        # 情感分析
        status_text.text("执行情感分析...")
        progress_bar.progress(60)
        df_sentiment = app.analyze_sentiment(df_processed)
        df_sentiment = df_sentiment.cache()
        
        # 话题挖掘
        status_text.text("挖掘热点话题...")
        progress_bar.progress(75)
        topic_results = app.mine_topics(df_sentiment, top_n=config['top_n'])
        
        # 趋势分析
        status_text.text("分析舆情趋势...")
        progress_bar.progress(90)
        trend_results = app.analyze_trend(df_sentiment, time_window=config['time_window'])
        
        # 完成
        status_text.text("分析完成！")
        progress_bar.progress(100)
        
        return {
            'data': df_sentiment,
            'topics': topic_results,
            'trends': trend_results
        }
        
    except Exception as e:
        st.error(f"分析过程中出现错误: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None


def render_data_overview(results):
    """渲染数据概览"""
    st.markdown('<div class="section-header">📋 数据概览</div>', unsafe_allow_html=True)
    
    # 转换为Pandas DataFrame用于展示
    df_pandas = results['data'].select(
        'doc_id', 'content', 'source', 'timestamp',
        'sentiment_label', 'sentiment_score'
    ).limit(1000).toPandas()
    
    # 统计指标
    total_count = results['data'].count()
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 总记录数", f"{total_count:,}")
    
    sentiment_counts = results['data'].groupBy('sentiment_label').count().collect()
    sentiment_dict = {row['sentiment_label']: row['count'] for row in sentiment_counts}
    
    with col2:
        st.metric("😊 正面", f"{sentiment_dict.get('positive', 0):,}", 
                  delta=f"{sentiment_dict.get('positive', 0)/total_count*100:.1f}%")
    
    with col3:
        st.metric("😐 中性", f"{sentiment_dict.get('neutral', 0):,}",
                  delta=f"{sentiment_dict.get('neutral', 0)/total_count*100:.1f}%")
    
    with col4:
        st.metric("😞 负面", f"{sentiment_dict.get('negative', 0):,}",
                  delta=f"{sentiment_dict.get('negative', 0)/total_count*100:.1f}%",
                  delta_color="inverse")
    
    # 数据预览
    st.markdown("#### 数据样本预览")
    st.dataframe(
        df_pandas[['doc_id', 'content', 'source', 'timestamp', 'sentiment_label', 'sentiment_score']],
        use_container_width=True,
        height=300
    )


def render_sentiment_analysis(results):
    """渲染情感分析结果"""
    st.markdown('<div class="section-header">😊 情感分析</div>', unsafe_allow_html=True)
    
    # 获取情感分布数据
    sentiment_df = results['data'].groupBy('sentiment_label').count().toPandas()
    sentiment_df.columns = ['情感', '数量']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 饼图
        st.markdown("#### 情感分布")
        fig_pie = px.pie(
            sentiment_df,
            values='数量',
            names='情感',
            color='情感',
            color_discrete_map={
                'positive': '#2ecc71',
                'neutral': '#95a5a6',
                'negative': '#e74c3c'
            },
            hole=0.4
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # 柱状图
        st.markdown("#### 情感统计")
        fig_bar = px.bar(
            sentiment_df,
            x='情感',
            y='数量',
            color='情感',
            color_discrete_map={
                'positive': '#2ecc71',
                'neutral': '#95a5a6',
                'negative': '#e74c3c'
            },
            text='数量'
        )
        fig_bar.update_traces(texttemplate='%{text:,}', textposition='outside')
        fig_bar.update_layout(
            height=400,
            showlegend=False,
            yaxis_title="数量"
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # 按来源的情感分布
    st.markdown("#### 各来源情感分布")
    source_sentiment_df = results['data'].groupBy('source', 'sentiment_label').count().toPandas()
    source_sentiment_df.columns = ['来源', '情感', '数量']
    
    fig_source = px.bar(
        source_sentiment_df,
        x='来源',
        y='数量',
        color='情感',
        color_discrete_map={
            'positive': '#2ecc71',
            'neutral': '#95a5a6',
            'negative': '#e74c3c'
        },
        barmode='group'
    )
    fig_source.update_layout(height=400)
    st.plotly_chart(fig_source, use_container_width=True)


def render_topic_mining(results):
    """渲染热点话题挖掘结果"""
    st.markdown('<div class="section-header">🔥 热点话题挖掘</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 高频词Top-N
        st.markdown("#### 高频词 Top-N")
        top_words_df = results['topics']['top_words'].toPandas()
        top_words_df.columns = ['词语', '频次']
        
        fig_words = px.bar(
            top_words_df.head(20),
            x='频次',
            y='词语',
            orientation='h',
            color='频次',
            color_continuous_scale='Blues'
        )
        fig_words.update_layout(
            height=500,
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False
        )
        st.plotly_chart(fig_words, use_container_width=True)
    
    with col2:
        # TF-IDF关键词
        st.markdown("#### TF-IDF 关键词")
        tfidf_df = results['topics']['tfidf_keywords'].toPandas()
        tfidf_df.columns = ['词语', 'TF-IDF分数']
        
        fig_tfidf = px.bar(
            tfidf_df.head(20),
            x='TF-IDF分数',
            y='词语',
            orientation='h',
            color='TF-IDF分数',
            color_continuous_scale='Oranges'
        )
        fig_tfidf.update_layout(
            height=500,
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False
        )
        st.plotly_chart(fig_tfidf, use_container_width=True)
    
    # 趋势词汇
    if results['topics']['trending'].count() > 0:
        st.markdown("#### 📈 趋势上升词汇")
        trending_df = results['topics']['trending'].toPandas()
        
        # 创建树状图
        fig_treemap = px.treemap(
            trending_df,
            path=['word'],
            values='recent_count',
            color='growth_rate',
            color_continuous_scale='RdYlGn',
            hover_data=['growth_rate']
        )
        fig_treemap.update_layout(height=400)
        st.plotly_chart(fig_treemap, use_container_width=True)


def render_trend_analysis(results):
    """渲染舆情趋势分析"""
    st.markdown('<div class="section-header">📈 舆情趋势分析</div>', unsafe_allow_html=True)
    
    # 时间趋势
    trend_df = results['trends']['sentiment_by_time'].toPandas()
    
    # 确保时间列转换为datetime
    time_col = [col for col in trend_df.columns if col.startswith('time_')][0]
    trend_df[time_col] = pd.to_datetime(trend_df[time_col])
    
    st.markdown("#### 情感随时间变化趋势")
    
    # 创建多线图
    fig_trend = go.Figure()
    
    fig_trend.add_trace(go.Scatter(
        x=trend_df[time_col],
        y=trend_df['positive_count'],
        mode='lines+markers',
        name='正面',
        line=dict(color='#2ecc71', width=3),
        marker=dict(size=8)
    ))
    
    fig_trend.add_trace(go.Scatter(
        x=trend_df[time_col],
        y=trend_df['neutral_count'],
        mode='lines+markers',
        name='中性',
        line=dict(color='#95a5a6', width=3),
        marker=dict(size=8)
    ))
    
    fig_trend.add_trace(go.Scatter(
        x=trend_df[time_col],
        y=trend_df['negative_count'],
        mode='lines+markers',
        name='负面',
        line=dict(color='#e74c3c', width=3),
        marker=dict(size=8)
    ))
    
    fig_trend.update_layout(
        height=400,
        xaxis_title="时间",
        yaxis_title="数量",
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig_trend, use_container_width=True)
    
    # 负面情绪比例趋势
    st.markdown("#### 负面情绪比例变化")
    trend_df['total'] = trend_df['positive_count'] + trend_df['neutral_count'] + trend_df['negative_count']
    trend_df['negative_ratio'] = trend_df['negative_count'] / trend_df['total']
    
    fig_negative = px.area(
        trend_df,
        x=time_col,
        y='negative_ratio',
        color_discrete_sequence=['#e74c3c']
    )
    fig_negative.update_layout(
        height=300,
        xaxis_title="时间",
        yaxis_title="负面情绪比例",
        yaxis_tickformat='.1%'
    )
    fig_negative.add_hline(
        y=0.4,
        line_dash="dash",
        line_color="red",
        annotation_text="预警阈值 (40%)"
    )
    st.plotly_chart(fig_negative, use_container_width=True)
    
    # 负面预警
    alerts_df = results['trends']['negative_alerts']
    if alerts_df.count() > 0:
        st.markdown("#### ⚠️ 负面舆情预警")
        alerts_pandas = alerts_df.toPandas()
        
        for _, row in alerts_pandas.iterrows():
            st.warning(
                f"**时间**: {row[time_col]} | "
                f"**负面数量**: {row['negative_count']} | "
                f"**负面比例**: {row['negative_ratio']:.2%} | "
                f"**总数**: {row['total_count']}"
            )
    else:
        st.success("✅ 当前无负面舆情预警")


def main():
    """主函数"""
    init_session_state()
    
    # 标题
    st.markdown('<div class="main-header">📊 舆情分析系统可视化平台</div>', unsafe_allow_html=True)
    st.markdown("---")
    
    # 侧边栏配置
    run_analysis, config = render_sidebar()
    
    # 运行分析
    if run_analysis:
        with st.spinner("正在分析中，请稍候..."):
            results = run_analysis_pipeline(config)
            if results:
                st.session_state.analysis_results = results
                st.session_state.data_loaded = True
                st.success("✅ 分析完成！")
    
    # 展示结果
    if st.session_state.analysis_results is not None:
        results = st.session_state.analysis_results
        
        # 创建标签页
        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 数据概览",
            "😊 情感分析",
            "🔥 热点话题",
            "📈 趋势分析"
        ])
        
        with tab1:
            render_data_overview(results)
        
        with tab2:
            render_sentiment_analysis(results)
        
        with tab3:
            render_topic_mining(results)
        
        with tab4:
            render_trend_analysis(results)
        
    else:
        # 欢迎页面
        st.info("👈 请在左侧配置参数后，点击「运行分析」开始分析")
        
        st.markdown("### 系统功能")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            #### 📥 数据处理
            - 支持CSV、JSON、Parquet格式
            - 模拟数据生成
            - 自动数据清洗与预处理
            - 智能去重与去噪
            
            #### 😊 情感分析
            - 基于情感词典的分析
            - 正面/中性/负面分类
            - 多维度情感统计
            - 按来源分析情感分布
            """)
        
        with col2:
            st.markdown("""
            #### 🔥 热点话题
            - 高频词统计
            - TF-IDF关键词提取
            - 趋势词汇识别
            - 话题演变追踪
            
            #### 📈 趋势分析
            - 时间序列分析
            - 负面情绪监控
            - 舆情预警机制
            - 多时间粒度聚合
            """)


if __name__ == "__main__":
    main()

