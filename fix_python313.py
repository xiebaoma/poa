"""
Python 3.13 + PySpark 快速修复脚本
自动安装缺失的依赖并应用补丁
"""
import sys
import subprocess

def check_and_install():
    """检查并安装必要的依赖"""
    print("=" * 60)
    print("Python 3.13 + PySpark 兼容性快速修复")
    print("=" * 60)
    print()
    
    # 检查 Python 版本
    python_version = sys.version_info
    print(f"当前 Python 版本: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version.major == 3 and python_version.minor >= 13:
        print("⚠️  检测到 Python 3.13+，需要额外的兼容性修复")
    
    print()
    
    # 需要安装的包
    required_packages = [
        ('setuptools', '65.0.0'),
        ('packaging', '21.0'),
    ]
    
    print("检查必要的依赖包...")
    print()
    
    missing_packages = []
    
    for package_name, min_version in required_packages:
        try:
            pkg = __import__(package_name)
            version = getattr(pkg, '__version__', '未知')
            print(f"✓ {package_name}: {version}")
        except ImportError:
            print(f"✗ {package_name}: 未安装")
            missing_packages.append(package_name)
    
    print()
    
    # 安装缺失的包
    if missing_packages:
        print("=" * 60)
        print("安装缺失的依赖包...")
        print("=" * 60)
        
        for package in missing_packages:
            print(f"\n安装 {package}...")
            try:
                subprocess.check_call([
                    sys.executable, '-m', 'pip', 'install', 
                    '--upgrade', package
                ])
                print(f"✓ {package} 安装成功")
            except subprocess.CalledProcessError as e:
                print(f"✗ {package} 安装失败: {e}")
                return False
    else:
        print("✓ 所有必要的依赖包已安装")
    
    print()
    print("=" * 60)
    print("应用兼容性补丁...")
    print("=" * 60)
    
    try:
        from pyspark_windows_compat import apply_all_patches
        apply_all_patches()
        print("✓ 补丁应用成功")
    except Exception as e:
        print(f"⚠️  补丁应用失败: {e}")
    
    print()
    print("=" * 60)
    print("测试 PySpark 导入...")
    print("=" * 60)
    
    try:
        import pyspark
        print(f"✓ PySpark {pyspark.__version__} 导入成功")
        
        # 测试 distutils
        try:
            from distutils.version import LooseVersion
            print("✓ distutils.version.LooseVersion 可用")
        except ImportError as e:
            print(f"⚠️  distutils 仍然不可用: {e}")
            print("   尝试重新安装 setuptools:")
            print("   pip install --force-reinstall setuptools")
    except Exception as e:
        print(f"✗ PySpark 导入失败: {e}")
        print("\n建议:")
        print("1. 卸载并重新安装 PySpark 3.5.x:")
        print("   pip uninstall pyspark -y")
        print("   pip install 'pyspark>=3.5.0,<4.0.0'")
        print("2. 或降级到 Python 3.11/3.12")
        return False
    
    print()
    print("=" * 60)
    print("✓ 修复完成！")
    print("=" * 60)
    print()
    print("你现在可以运行:")
    print("  python test_pyspark_windows.py  # 完整测试")
    print("  python scripts/quick_start.py    # 快速开始")
    print()
    
    return True


if __name__ == "__main__":
    success = check_and_install()
    sys.exit(0 if success else 1)

