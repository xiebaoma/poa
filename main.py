# -*- coding: utf-8 -*-
"""
Python+Spark 热词分析系统

主程序入口

"""

import os
import sys

# 确保项目根目录在路径中
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def check_dependencies():
    """检查依赖"""
    missing = []
    
    try:
        import pandas
    except ImportError:
        missing.append("pandas")
    
    try:
        import jieba
    except ImportError:
        missing.append("jieba")
    
    try:
        import snownlp
    except ImportError:
        missing.append("snownlp")
    
    try:
        import matplotlib
    except ImportError:
        missing.append("matplotlib")
    
    try:
        import wordcloud
    except ImportError:
        missing.append("wordcloud")
    
    try:
        from PIL import Image
    except ImportError:
        missing.append("Pillow")
    
    # PySpark 是可选的
    try:
        import pyspark
        print("✓ PySpark 已安装")
    except ImportError:
        print("⚠ PySpark 未安装，将使用 Pandas 进行数据处理")
    
    # ttkbootstrap 是可选的
    try:
        import ttkbootstrap
        print("✓ ttkbootstrap 已安装")
    except ImportError:
        print("⚠ ttkbootstrap 未安装，将使用标准 ttk 主题")
    
    if missing:
        print(f"\n缺少以下依赖: {', '.join(missing)}")
        print("请运行以下命令安装:")
        print(f"  pip install {' '.join(missing)}")
        return False
    
    return True


def main():
    """主函数"""
    print("=" * 60)
    print("  Python+Spark 热词分析系统")
    print("=" * 60)
    print()
    
    # 检查依赖
    print("检查依赖...")
    if not check_dependencies():
        print("\n请安装缺少的依赖后重试")
        input("按 Enter 键退出...")
        return
    
    print("\n启动 GUI 界面...\n")
    
    # 启动GUI
    try:
        from gui import main as gui_main
        gui_main()
    except Exception as e:
        print(f"\n启动失败: {e}")
        import traceback
        traceback.print_exc()
        input("\n按 Enter 键退出...")


if __name__ == "__main__":
    main()
