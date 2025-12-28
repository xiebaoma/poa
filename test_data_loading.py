"""
测试数据加载功能
"""
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src/main/python"))

print("=" * 60)
print("测试数据加载")
print("=" * 60)
print()

# 应用 Windows 修复
import platform
if platform.system() == 'Windows':
    try:
        from fix_spark_windows import fix_spark_windows_paths
        fix_spark_windows_paths()
        print()
    except Exception as e:
        print(f"警告: {e}\n")

# 应用 Python 3.13 修复
if sys.version_info >= (3, 12):
    try:
        from pyspark_windows_compat import apply_python313_patch
        apply_python313_patch()
        print()
    except Exception as e:
        print(f"警告: {e}\n")

try:
    from data.loader import DataLoader
    from utils.config_loader import get_data_paths
    
    # 检查数据路径
    paths = get_data_paths()
    raw_path = paths['raw_path']
    
    print(f"数据路径: {raw_path}")
    print(f"路径存在: {raw_path.exists()}")
    
    # 列出 CSV 文件
    csv_files = list(raw_path.glob("*.csv"))
    print(f"找到 {len(csv_files)} 个 CSV 文件:")
    for f in csv_files:
        size = f.stat().st_size / 1024  # KB
        print(f"  - {f.name} ({size:.1f} KB)")
    print()
    
    if not csv_files:
        print("✗ 没有找到 CSV 文件！")
        print("请先运行: python scripts/test_data_layer.py")
        sys.exit(1)
    
    # 创建 DataLoader
    print("创建 DataLoader...")
    loader = DataLoader()
    print("✓ DataLoader 创建成功")
    print()
    
    # 测试加载单个文件
    print(f"测试加载文件: {csv_files[0].name}")
    print(f"完整路径: {csv_files[0]}")
    print(f"绝对路径: {csv_files[0].resolve()}")
    
    if platform.system() == 'Windows':
        print(f"URI 格式: {csv_files[0].as_uri()}")
    print()
    
    try:
        df = loader.load_from_csv(csv_files[0])
        count = df.count()
        print(f"✓ 成功加载 {count} 条记录")
        
        # 显示样本数据
        print("\n样本数据:")
        df.show(5, truncate=50)
        
    except Exception as e:
        print(f"✗ 加载失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # 测试加载所有文件
    print("\n" + "=" * 60)
    print("测试加载所有文件")
    print("=" * 60)
    
    try:
        df_all = loader.load_raw_data(file_format='csv')
        count_all = df_all.count()
        print(f"✓ 成功加载所有文件，共 {count_all} 条记录")
    except Exception as e:
        print(f"✗ 加载所有文件失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print()
    print("=" * 60)
    print("✓ 所有测试通过！")
    print("=" * 60)
    
except Exception as e:
    print(f"✗ 测试失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

