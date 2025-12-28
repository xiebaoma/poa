"""
PySpark Windows 兼容性补丁
解决 PySpark 4.x 在 Windows 上的 UnixStreamServer 问题
解决 Python 3.13 移除 distutils 的问题
"""
import sys
import platform

def apply_pyspark_windows_patch():
    """
    为 Windows 系统应用 PySpark 兼容性补丁
    """
    if platform.system() != 'Windows':
        return
    
    try:
        import socketserver
        
        # 如果 UnixStreamServer 不存在，创建一个替代类
        if not hasattr(socketserver, 'UnixStreamServer'):
            # 使用 TCP Server 作为替代
            class UnixStreamServer(socketserver.TCPServer):
                """Windows 兼容的 UnixStreamServer 替代实现"""
                
                def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
                    # 在 Windows 上使用 localhost TCP 连接
                    if isinstance(server_address, str):
                        # 如果是 Unix socket 路径，转换为 TCP 地址
                        import random
                        port = random.randint(50000, 60000)
                        server_address = ('127.0.0.1', port)
                    
                    super().__init__(server_address, RequestHandlerClass, bind_and_activate)
            
            # 将替代类添加到 socketserver 模块
            socketserver.UnixStreamServer = UnixStreamServer
            
            print("已应用 PySpark Windows 兼容性补丁（UnixStreamServer）")
            
    except Exception as e:
        print(f"应用 PySpark 兼容性补丁时出错: {e}", file=sys.stderr)


def apply_python313_patch():
    """
    为 Python 3.13 应用 distutils 兼容性补丁
    Python 3.13 移除了 distutils，但 PySpark 仍然依赖它
    """
    python_version = sys.version_info
    
    # 只在 Python 3.12+ 上应用
    if python_version.major == 3 and python_version.minor >= 12:
        try:
            # 检查 distutils 是否存在
            import distutils
        except ImportError:
            # distutils 不存在，创建一个兼容层
            try:
                # 使用 setuptools 提供的 distutils（如果可用）
                import setuptools
                import setuptools._distutils as distutils
                sys.modules['distutils'] = distutils
                sys.modules['distutils.version'] = distutils.version
                print("已应用 Python 3.13 兼容性补丁（distutils -> setuptools._distutils）")
            except ImportError:
                # setuptools 也不可用，手动创建最小实现
                try:
                    from packaging import version as pkg_version
                    
                    # 创建一个模拟的 distutils 模块
                    class MockDistutils:
                        class version:
                            @staticmethod
                            def LooseVersion(v):
                                return pkg_version.parse(str(v))
                    
                    sys.modules['distutils'] = MockDistutils()
                    sys.modules['distutils.version'] = MockDistutils.version
                    print("已应用 Python 3.13 兼容性补丁（distutils -> packaging.version）")
                except ImportError:
                    print("警告: 无法完全修复 distutils 依赖，请安装 setuptools 或 packaging", file=sys.stderr)
                    print("运行: pip install setuptools packaging", file=sys.stderr)


def apply_all_patches():
    """
    应用所有兼容性补丁
    """
    if platform.system() == 'Windows':
        apply_pyspark_windows_patch()
    
    apply_python313_patch()


def check_pyspark_compatibility():
    """
    检查当前环境的 PySpark 兼容性
    """
    issues = []
    
    # 检查操作系统
    if platform.system() == 'Windows':
        issues.append("检测到 Windows 系统")
        
        # 检查 Python 版本
        python_version = sys.version_info
        if python_version.major == 3 and python_version.minor >= 13:
            issues.append(f"Python {python_version.major}.{python_version.minor} 在 Windows 上可能与 PySpark 4.x 存在兼容性问题")
        
        # 检查 PySpark 版本
        try:
            import pyspark
            version = pyspark.__version__
            major_version = int(version.split('.')[0])
            
            if major_version >= 4:
                issues.append(f"PySpark {version} 在 Windows 上可能需要兼容性补丁")
            else:
                issues.append(f"PySpark {version} - 版本兼容")
                
        except ImportError:
            issues.append("PySpark 未安装")
    
    return issues


if __name__ == "__main__":
    print("=== PySpark Windows 兼容性检查 ===")
    issues = check_pyspark_compatibility()
    
    for issue in issues:
        print(f"- {issue}")
    
    print("\n应用兼容性补丁...")
    apply_all_patches()
    
    print("\n建议:")
    print("1. 使用 PySpark 3.5.x 版本（最稳定）")
    print("2. 或者降级到 Python 3.11 或 3.12")
    print("3. Python 3.13 用户: 确保已安装 setuptools 或 packaging")
    print("   pip install setuptools packaging")
    print("4. 在代码开头导入此模块: from pyspark_windows_compat import apply_all_patches")

