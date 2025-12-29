# -*- coding: utf-8 -*-
"""
环境检查脚本
用于检查运行环境是否配置正确
"""
import sys

def check_python_version():
    """检查 Python 版本"""
    version = sys.version_info
    print(f"Python 版本: {version.major}.{version.minor}.{version.micro}")
    if version.major < 3 or (version.major == 3 and version.minor < 7):
        print("  ❌ Python 版本过低，需要 Python 3.7+")
        return False
    print("  ✓ Python 版本符合要求")
    return True

def check_dependencies():
    """检查依赖包"""
    dependencies = {
        'pandas': 'pandas',
        'jieba': 'jieba',
        'snownlp': 'snownlp',
        'matplotlib': 'matplotlib',
        'wordcloud': 'wordcloud',
        'PIL': 'Pillow',
        'pyspark': 'pyspark (可选)',
        'ttkbootstrap': 'ttkbootstrap (可选)'
    }
    
    missing = []
    optional_missing = []
    
    for module, name in dependencies.items():
        try:
            if module == 'PIL':
                __import__('PIL')
            else:
                __import__(module)
            print(f"  ✓ {name}")
        except ImportError:
            if '可选' in name:
                print(f"  ⚠ {name} - 未安装（可选）")
                optional_missing.append(name)
            else:
                print(f"  ❌ {name} - 未安装")
                missing.append(name)
    
    return missing, optional_missing

def check_data_files():
    """检查数据文件"""
    import os
    
    files = {
        'data/covid_weibo_train.csv': 'COVID-19 微博数据集',
        'data/stopwords.txt': '停用词文件'
    }
    
    all_exist = True
    for file_path, desc in files.items():
        if os.path.exists(file_path):
            print(f"  ✓ {desc}: {file_path}")
        else:
            print(f"  ❌ {desc}: {file_path} - 文件不存在")
            all_exist = False
    
    return all_exist

def check_java():
    """检查 Java 环境（用于 Spark）"""
    import os
    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        print(f"  ✓ JAVA_HOME: {java_home}")
        return True
    else:
        print("  ⚠ JAVA_HOME 未设置（Spark 功能可能不可用）")
        return False

def main():
    print("=" * 60)
    print("  Python+Spark 热词分析系统 - 环境检查")
    print("=" * 60)
    print()
    
    # 检查 Python 版本
    print("[1] 检查 Python 版本...")
    if not check_python_version():
        print("\n请升级 Python 到 3.7 或更高版本")
        return
    print()
    
    # 检查依赖
    print("[2] 检查依赖包...")
    missing, optional_missing = check_dependencies()
    print()
    
    if missing:
        print("缺少以下必需依赖:")
        for dep in missing:
            print(f"  - {dep}")
        print("\n请运行以下命令安装:")
        print("  pip install -r requirements.txt")
        print("或使用国内镜像:")
        print("  pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple")
        return
    print()
    
    # 检查数据文件
    print("[3] 检查数据文件...")
    if not check_data_files():
        print("\n请确保数据文件存在")
        return
    print()
    
    # 检查 Java
    print("[4] 检查 Java 环境（可选）...")
    check_java()
    print()
    
    # 总结
    print("=" * 60)
    print("  环境检查完成！")
    print("=" * 60)
    
    if missing:
        print("\n❌ 环境配置不完整，请先安装缺少的依赖")
    elif optional_missing:
        print("\n✓ 基本环境已配置，可以运行程序")
        print("  部分可选功能可能不可用，但不影响基本使用")
    else:
        print("\n✓ 环境配置完整，可以正常运行程序")
        print("\n运行程序:")
        print("  python main.py")
        print("或双击: 快速启动.bat")

if __name__ == "__main__":
    main()

