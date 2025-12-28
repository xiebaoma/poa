"""
修复 Spark 在 Windows 上的 "系统找不到指定的路径" 错误
这个错误通常是因为 Spark 需要访问临时目录，但 Windows 上缺少必要的配置
"""
import os
import sys
from pathlib import Path
import tempfile
import subprocess
import urllib.request
import stat

def find_java_home():
    """
    自动查找 Java 安装位置
    """
    # 1. 检查环境变量
    if 'JAVA_HOME' in os.environ:
        java_home = Path(os.environ['JAVA_HOME'])
        if java_home.exists() and (java_home / 'bin' / 'java.exe').exists():
            return java_home
    
    # 2. 尝试从 PATH 中找到 java
    try:
        result = subprocess.run(['where', 'java'], 
                              capture_output=True, 
                              text=True, 
                              timeout=5)
        if result.returncode == 0:
            java_exe = Path(result.stdout.strip().split('\n')[0])
            # java.exe -> bin -> jdk
            java_home = java_exe.parent.parent
            # 验证这是一个有效的 Java 目录
            if java_home.exists() and (java_home / 'bin' / 'java.exe').exists():
                # 排除一些不标准的 Java 路径（如 Common Files）
                if 'Common Files' not in str(java_home):
                    return java_home
    except:
        pass
    
    # 3. 查找常见安装位置
    common_paths = [
        Path(r"D:\java"),  # 用户自定义安装位置
        Path(r"D:\Java"),
        Path(r"C:\Program Files\Eclipse Adoptium"),
        Path(r"C:\Program Files\Java"),
        Path(r"C:\Program Files (x86)\Java"),
        Path(r"C:\Program Files\OpenJDK"),
        Path(r"C:\Program Files\Microsoft"),  # 有时微软会在这里安装 OpenJDK
        Path(r"C:\Java"),
    ]
    
    for base_path in common_paths:
        if not base_path.exists():
            continue
        
        # 先检查该路径本身是否就是 Java 目录
        if (base_path / 'bin' / 'java.exe').exists():
            return base_path
        
        # 如果不是，则查找子目录中的 jdk 或 jre 目录
        try:
            for item in base_path.iterdir():
                if item.is_dir():
                    # 检查是否是 Java 目录
                    name_lower = item.name.lower()
                    if any(keyword in name_lower for keyword in ['jdk', 'jre', 'java', 'openjdk']):
                        if (item / 'bin' / 'java.exe').exists():
                            return item
        except PermissionError:
            # 跳过没有权限访问的目录
            continue
    
    return None

def download_winutils(hadoop_home):
    """
    下载 winutils.exe 和 hadoop.dll 到 Hadoop bin 目录
    """
    bin_dir = hadoop_home / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    
    winutils_path = bin_dir / "winutils.exe"
    hadoop_dll_path = bin_dir / "hadoop.dll"
    
    # 如果都已存在，不需要重新下载
    if winutils_path.exists() and hadoop_dll_path.exists():
        print(f"  ✓ winutils.exe 和 hadoop.dll 已存在")
        return True
    
    print("  ⏳ 正在下载 Hadoop 工具...")
    
    # winutils.exe 和 hadoop.dll 的下载地址（Hadoop 3.3.5 版本）
    # 参考: https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin
    base_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/"
    files_to_download = {
        "winutils.exe": winutils_path,
        "hadoop.dll": hadoop_dll_path
    }
    
    success = True
    for filename, local_path in files_to_download.items():
        if local_path.exists():
            print(f"  ✓ {filename} 已存在")
            continue
            
        url = base_url + filename
        try:
            print(f"  ⏳ 下载 {filename}...")
            urllib.request.urlretrieve(url, str(local_path))
            
            # 设置执行权限
            if filename.endswith('.exe'):
                local_path.chmod(local_path.stat().st_mode | stat.S_IEXEC)
            
            print(f"  ✓ {filename} 已下载")
        except Exception as e:
            print(f"  ⚠️  下载 {filename} 失败: {e}")
            print(f"  提示: 您可以手动从以下地址下载:")
            print(f"       {url}")
            print(f"       并保存到: {local_path}")
            success = False
    
    if success:
        print(f"  ✓ Hadoop 工具已安装到: {bin_dir}")
    
    return success


def fix_spark_windows_paths():
    """
    修复 Spark 在 Windows 上的路径问题
    """
    print("=" * 60)
    print("修复 Spark Windows 路径配置")
    print("=" * 60)
    print()
    
    # 0. 检查并设置 JAVA_HOME
    java_home = find_java_home()
    if java_home:
        # 使用绝对路径，并确保格式正确
        java_home_str = str(java_home.resolve())
        os.environ['JAVA_HOME'] = java_home_str
        
        # 确保 Java bin 目录在 PATH 中
        java_bin = str((java_home / 'bin').resolve())
        current_path = os.environ.get('PATH', '')
        if java_bin not in current_path:
            os.environ['PATH'] = f"{java_bin};{current_path}"
        
        print(f"✓ 找到 Java: {java_home_str}")
        print(f"✓ 设置 JAVA_HOME: {java_home_str}")
        
        # 验证 Java 可执行
        try:
            result = subprocess.run([str(java_home / 'bin' / 'java.exe'), '-version'], 
                                  capture_output=True, 
                                  text=True, 
                                  timeout=5)
            print(f"✓ Java 可正常执行")
        except Exception as e:
            print(f"⚠️  警告: Java 可能无法正常执行: {e}")
        print()
    else:
        print("⚠️  警告: 未找到标准的 Java 安装")
        print("   PySpark 需要 Java 才能运行")
        print("   请安装 Java: https://adoptium.net/")
        print()
    
    # 1. 设置临时目录 - 使用用户目录下的路径避免权限问题
    user_home = Path.home()
    temp_base = user_home / "spark-temp"
    temp_base.mkdir(parents=True, exist_ok=True)
    
    # 2. 创建 Spark 本地目录
    project_root = Path(__file__).parent.resolve()
    spark_local_dir = project_root / ".spark-local"
    spark_local_dir.mkdir(parents=True, exist_ok=True)
    
    # 3. 创建 Spark warehouse 目录
    spark_warehouse = project_root / "spark-warehouse"
    spark_warehouse.mkdir(parents=True, exist_ok=True)
    
    # 4. 创建 Derby 数据库目录（避免路径问题）
    derby_dir = project_root / ".derby"
    derby_dir.mkdir(parents=True, exist_ok=True)
    
    print("创建的目录:")
    print(f"  ✓ Spark 临时目录: {temp_base}")
    print(f"  ✓ Spark 本地目录: {spark_local_dir}")
    print(f"  ✓ Spark warehouse: {spark_warehouse}")
    print(f"  ✓ Derby 目录: {derby_dir}")
    print()
    
    # 5. 设置环境变量 - 使用绝对路径
    env_vars = {
        'SPARK_LOCAL_DIRS': str(spark_local_dir.resolve()),
        'SPARK_TEMP_DIR': str(temp_base.resolve()),
        'JAVA_IO_TMPDIR': str(temp_base.resolve()),
        'TMP': str(temp_base.resolve()),
        'TEMP': str(temp_base.resolve()),
        'HADOOP_USER_NAME': 'spark',  # 禁用 Hadoop 用户认证
    }
    
    # 如果没有 HADOOP_HOME，创建一个模拟的
    if 'HADOOP_HOME' not in os.environ:
        hadoop_home = project_root / ".hadoop"
        hadoop_home.mkdir(parents=True, exist_ok=True)
        hadoop_bin = hadoop_home / "bin"
        hadoop_bin.mkdir(parents=True, exist_ok=True)
        
        # 下载 winutils.exe
        download_winutils(hadoop_home)
        
        env_vars['HADOOP_HOME'] = str(hadoop_home.resolve())
        print(f"  ✓ 创建 HADOOP_HOME: {hadoop_home}")
    
    for key, value in env_vars.items():
        os.environ[key] = value
        print(f"  ✓ 设置环境变量 {key}: {value}")
    
    print()
    print("=" * 60)
    print("✓ Spark Windows 路径配置完成")
    print("=" * 60)
    print()


def get_spark_config_for_windows():
    """
    获取适用于 Windows 的 Spark 配置
    
    Returns:
        dict: Spark 配置字典
    """
    project_root = Path(__file__).parent.resolve()
    temp_dir = Path.home() / "spark-temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    hadoop_home = project_root / ".hadoop"
    hadoop_home.mkdir(parents=True, exist_ok=True)
    
    # Java 17+ 兼容性参数
    java_opts = [
        f"-Djava.io.tmpdir={temp_dir}",
        f"-Dhadoop.home.dir={hadoop_home}",
        "-DHADOOP_USER_NAME=spark",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
        "-XX:+IgnoreUnrecognizedVMOptions"
    ]
    java_opts_str = " ".join(java_opts)
    
    config = {
        "spark.local.dir": str((project_root / ".spark-local").resolve()),
        "spark.sql.warehouse.dir": str((project_root / "spark-warehouse").resolve()),
        "spark.driver.extraJavaOptions": java_opts_str,
        "spark.executor.extraJavaOptions": java_opts_str,
        # Windows 特定配置
        "spark.sql.catalogImplementation": "in-memory",
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.driver.host": "localhost",
        "spark.ui.enabled": "false",
        # 禁用 Hadoop 安全认证
        "spark.hadoop.hadoop.security.authentication": "simple",
        "spark.hadoop.hadoop.security.authorization": "false",
        "spark.authenticate": "false",
        # 禁用一些可能导致问题的功能
        "spark.sql.hive.metastore.sharedPrefixes": "",
        "spark.sql.hive.metastore.barrierPrefixes": "",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    }
    
    return config


if __name__ == "__main__":
    fix_spark_windows_paths()
    
    print("\n建议:")
    print("1. 在创建 SparkSession 之前调用 fix_spark_windows_paths()")
    print("2. 或在代码开头导入:")
    print("   from fix_spark_windows import fix_spark_windows_paths")
    print("   fix_spark_windows_paths()")
    print()
    print("3. 创建 SparkSession 时使用额外配置:")
    print("   from fix_spark_windows import get_spark_config_for_windows")
    print("   config = get_spark_config_for_windows()")
    print("   for key, value in config.items():")
    print("       builder = builder.config(key, value)")

