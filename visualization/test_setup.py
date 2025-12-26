"""
测试可视化系统环境配置
检查所有依赖是否正确安装
"""
import sys
from pathlib import Path

def test_imports():
    """测试必要的包导入"""
    print("=" * 60)
    print("测试依赖包导入...")
    print("=" * 60)
    
    packages = {
        'streamlit': 'Streamlit Web框架',
        'plotly': 'Plotly可视化库',
        'pandas': 'Pandas数据处理',
        'pyspark': 'PySpark分布式计算',
        'yaml': 'YAML配置解析'
    }
    
    failed = []
    
    for package, description in packages.items():
        try:
            if package == 'yaml':
                __import__('yaml')
            else:
                __import__(package)
            print(f"✓ {package:15s} - {description}")
        except ImportError as e:
            print(f"✗ {package:15s} - {description} (未安装)")
            failed.append(package)
    
    print()
    
    if failed:
        print("❌ 以下包未安装:")
        for pkg in failed:
            print(f"   - {pkg}")
        print("\n请运行以下命令安装:")
        print("   pip install -r requirements.txt")
        return False
    else:
        print("✅ 所有依赖包已正确安装！")
        return True


def test_project_structure():
    """测试项目结构"""
    print("\n" + "=" * 60)
    print("测试项目结构...")
    print("=" * 60)
    
    project_root = Path(__file__).parent.parent
    
    required_paths = {
        'src/main/python': '源代码目录',
        'src/main/resources/config': '配置文件目录',
        'src/main/resources/dict': '词典目录',
        'data/raw': '原始数据目录',
        'data/processed': '处理后数据目录',
        'data/results': '结果目录',
        'visualization': '可视化目录'
    }
    
    missing = []
    
    for path, description in required_paths.items():
        full_path = project_root / path
        if full_path.exists():
            print(f"✓ {path:35s} - {description}")
        else:
            print(f"✗ {path:35s} - {description} (不存在)")
            missing.append(path)
    
    print()
    
    if missing:
        print("⚠️ 以下目录不存在，将自动创建:")
        for path in missing:
            full_path = project_root / path
            full_path.mkdir(parents=True, exist_ok=True)
            print(f"   ✓ 已创建: {path}")
        print()
    
    print("✅ 项目结构检查完成！")
    return True


def test_config_files():
    """测试配置文件"""
    print("\n" + "=" * 60)
    print("测试配置文件...")
    print("=" * 60)
    
    project_root = Path(__file__).parent.parent
    
    config_files = {
        'src/main/resources/config/config.yaml': '主配置文件',
        'src/main/resources/dict/stopwords.txt': '停用词词典',
        'src/main/resources/dict/positive_words.txt': '正面词词典',
        'src/main/resources/dict/negative_words.txt': '负面词词典'
    }
    
    missing = []
    
    for file_path, description in config_files.items():
        full_path = project_root / file_path
        if full_path.exists():
            size = full_path.stat().st_size
            print(f"✓ {file_path:50s} ({size:>6d} bytes)")
        else:
            print(f"✗ {file_path:50s} (不存在)")
            missing.append(file_path)
    
    print()
    
    if missing:
        print("❌ 以下配置文件缺失:")
        for path in missing:
            print(f"   - {path}")
        print("\n请确保配置文件存在")
        return False
    else:
        print("✅ 所有配置文件检查完成！")
        return True


def test_streamlit_version():
    """测试Streamlit版本"""
    print("\n" + "=" * 60)
    print("测试Streamlit版本...")
    print("=" * 60)
    
    try:
        import streamlit as st
        version = st.__version__
        print(f"Streamlit版本: {version}")
        
        # 检查版本是否符合要求
        major, minor = map(int, version.split('.')[:2])
        if major >= 1 and minor >= 28:
            print("✅ Streamlit版本符合要求 (>= 1.28.0)")
            return True
        else:
            print("⚠️ Streamlit版本过低，建议升级到 1.28.0+")
            print("   运行: pip install --upgrade streamlit")
            return False
    except Exception as e:
        print(f"❌ 检查Streamlit版本失败: {e}")
        return False


def test_spark_setup():
    """测试Spark配置"""
    print("\n" + "=" * 60)
    print("测试Spark配置...")
    print("=" * 60)
    
    try:
        from pyspark.sql import SparkSession
        
        # 尝试创建SparkSession
        print("正在创建SparkSession (可能需要几秒)...")
        spark = SparkSession.builder \
            .appName("ConfigTest") \
            .master("local[1]") \
            .config("spark.driver.memory", "512m") \
            .getOrCreate()
        
        # 测试基本操作
        df = spark.createDataFrame([("test", 1)], ["text", "value"])
        count = df.count()
        
        spark.stop()
        
        print("✅ Spark配置正常，可以正常运行")
        return True
        
    except Exception as e:
        print(f"❌ Spark配置失败: {e}")
        print("\n可能的原因:")
        print("   1. Java未安装或未配置JAVA_HOME")
        print("   2. PySpark版本不兼容")
        print("   3. 内存不足")
        return False


def main():
    """主测试函数"""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 15 + "舆情分析系统环境测试" + " " * 15 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    results = []
    
    # 运行所有测试
    results.append(("依赖包导入", test_imports()))
    results.append(("项目结构", test_project_structure()))
    results.append(("配置文件", test_config_files()))
    results.append(("Streamlit版本", test_streamlit_version()))
    results.append(("Spark配置", test_spark_setup()))
    
    # 总结
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{name:20s} : {status}")
    
    print()
    print(f"总计: {passed}/{total} 项测试通过")
    
    if passed == total:
        print("\n" + "🎉" * 20)
        print("恭喜！所有测试通过，系统配置正确！")
        print("现在可以启动Web可视化界面:")
        print("   ./visualization/run_web.sh    (Mac/Linux)")
        print("   visualization\\run_web.bat      (Windows)")
        print("🎉" * 20)
        return 0
    else:
        print("\n⚠️ 部分测试未通过，请检查上述错误信息")
        print("建议:")
        print("   1. 运行: pip install -r requirements.txt")
        print("   2. 确保Java已安装 (用于PySpark)")
        print("   3. 检查配置文件是否完整")
        return 1


if __name__ == "__main__":
    sys.exit(main())

