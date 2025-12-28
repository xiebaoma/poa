"""
测试 PySpark 在 Windows 上的兼容性
验证修复是否生效
"""
import sys
import platform

def test_pyspark_import():
    """测试 PySpark 是否能正常导入"""
    print("=" * 60)
    print("测试 PySpark 导入")
    print("=" * 60)
    
    try:
        # 首先应用兼容性补丁
        if platform.system() == 'Windows':
            print("检测到 Windows 系统，应用兼容性补丁...")
            try:
                from pyspark_windows_compat import apply_all_patches
                apply_all_patches()
            except Exception as e:
                print(f"警告: 无法应用补丁: {e}")
        
        # Python 3.13 补丁
        if sys.version_info >= (3, 12):
            print("检测到 Python 3.12+，应用 distutils 兼容性补丁...")
            try:
                from pyspark_windows_compat import apply_python313_patch
                apply_python313_patch()
            except Exception as e:
                print(f"警告: 无法应用 Python 3.13 补丁: {e}")
        
        # 测试 distutils
        print("\n测试 distutils 依赖...")
        try:
            from distutils.version import LooseVersion
            print("✓ distutils.version.LooseVersion 可用")
        except ImportError as e:
            print(f"✗ distutils 不可用: {e}")
            print("  建议运行: python fix_python313.py")
            return False
        
        # 尝试导入 PySpark
        print("\n导入 PySpark...")
        import pyspark
        from pyspark.sql import SparkSession
        
        print(f"✓ PySpark 导入成功！")
        print(f"  版本: {pyspark.__version__}")
        
        return True
    except Exception as e:
        print(f"✗ PySpark 导入失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_spark_session():
    """测试 SparkSession 是否能正常创建"""
    print("\n" + "=" * 60)
    print("测试 SparkSession 创建")
    print("=" * 60)
    
    try:
        from pyspark.sql import SparkSession
        
        print("创建 SparkSession...")
        spark = SparkSession.builder \
            .appName("CompatibilityTest") \
            .master("local[1]") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        
        print("✓ SparkSession 创建成功！")
        print(f"  Spark 版本: {spark.version}")
        
        # 测试简单操作
        print("\n测试简单 DataFrame 操作...")
        df = spark.createDataFrame([
            (1, "测试1"),
            (2, "测试2"),
            (3, "测试3")
        ], ["id", "value"])
        
        count = df.count()
        print(f"✓ DataFrame 操作成功！记录数: {count}")
        
        # 停止 Spark
        spark.stop()
        print("✓ SparkSession 已停止")
        
        return True
    except Exception as e:
        print(f"✗ SparkSession 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_environment():
    """测试环境信息"""
    print("\n" + "=" * 60)
    print("环境信息")
    print("=" * 60)
    
    print(f"操作系统: {platform.system()} {platform.release()}")
    print(f"Python 版本: {sys.version}")
    print(f"Python 路径: {sys.executable}")
    
    # 检查关键模块
    modules = ['pyspark', 'pandas', 'numpy', 'yaml']
    print("\n已安装的关键模块:")
    for module in modules:
        try:
            mod = __import__(module)
            version = getattr(mod, '__version__', '未知')
            print(f"  ✓ {module}: {version}")
        except ImportError:
            print(f"  ✗ {module}: 未安装")


def main():
    """主测试函数"""
    print("\n" + "=" * 60)
    print("PySpark Windows 兼容性测试")
    print("=" * 60)
    print()
    
    # 测试环境
    test_environment()
    
    # 测试 PySpark 导入
    import_ok = test_pyspark_import()
    
    if not import_ok:
        print("\n" + "=" * 60)
        print("测试失败！")
        print("=" * 60)
        print("\n建议:")
        print("1. 运行快速修复脚本:")
        print("   python fix_python313.py")
        print("2. 确保已安装正确版本的 PySpark:")
        print("   pip install 'pyspark>=3.5.0,<4.0.0'")
        print("3. 确保已安装 setuptools 和 packaging:")
        print("   pip install setuptools packaging")
        print("4. 如果使用 Python 3.13，建议降级到 3.11 或 3.12")
        print("5. 查看详细说明: PYSPARK_WINDOWS_FIX.md")
        return False
    
    # 测试 SparkSession
    session_ok = test_spark_session()
    
    print("\n" + "=" * 60)
    if import_ok and session_ok:
        print("✓ 所有测试通过！")
        print("=" * 60)
        print("\nPySpark 在你的系统上运行正常。")
        print("你可以开始使用舆情分析系统了。")
        print("\n快速开始:")
        print("  python scripts/quick_start.py")
        return True
    else:
        print("✗ 部分测试失败")
        print("=" * 60)
        print("\n请检查上述错误信息并参考 PYSPARK_WINDOWS_FIX.md")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

