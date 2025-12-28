"""
诊断数据路径配置问题
"""
from pathlib import Path
import sys

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src/main/python"))

print("=" * 60)
print("数据路径诊断")
print("=" * 60)
print()

# 当前文件位置
print(f"当前文件: {__file__}")
print(f"项目根目录: {project_root}")
print()

# 导入配置加载器
try:
    from utils.config_loader import load_config, get_data_paths
    
    print("✓ 成功导入 config_loader")
    print()
    
    # 加载配置
    config = load_config()
    print("配置文件内容:")
    print(f"  raw_path: {config.get('data', {}).get('raw_path')}")
    print(f"  processed_path: {config.get('data', {}).get('processed_path')}")
    print(f"  results_path: {config.get('data', {}).get('results_path')}")
    print()
    
    # 获取路径
    paths = get_data_paths(config)
    print("计算出的路径:")
    print(f"  raw_path: {paths['raw_path']}")
    print(f"  processed_path: {paths['processed_path']}")
    print(f"  results_path: {paths['results_path']}")
    print()
    
    # 检查路径是否存在
    print("路径存在性检查:")
    for name, path in paths.items():
        exists = path.exists()
        status = "✓" if exists else "✗"
        print(f"  {status} {name}: {path} ({'存在' if exists else '不存在'})")
    
    print()
    
    # 检查数据文件
    raw_path = paths['raw_path']
    if raw_path.exists():
        csv_files = list(raw_path.glob("*.csv"))
        print(f"在 {raw_path} 中找到 {len(csv_files)} 个 CSV 文件:")
        for f in csv_files:
            print(f"  - {f.name}")
    else:
        print(f"✗ 原始数据目录不存在: {raw_path}")
        print(f"  需要创建目录")
    
    print()
    
    # 检查错误位置
    wrong_path = project_root / "src/data/raw"
    if wrong_path.exists():
        wrong_files = list(wrong_path.glob("*.csv"))
        if wrong_files:
            print(f"⚠️  发现文件在错误位置: {wrong_path}")
            print(f"   找到 {len(wrong_files)} 个 CSV 文件:")
            for f in wrong_files:
                print(f"  - {f.name}")
            print()
            print(f"建议: 移动这些文件到 {raw_path}")
    
except Exception as e:
    print(f"✗ 错误: {e}")
    import traceback
    traceback.print_exc()

print()
print("=" * 60)
print("诊断完成")
print("=" * 60)

