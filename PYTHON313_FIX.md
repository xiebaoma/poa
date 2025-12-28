# Python 3.13 快速修复指南

## 问题

如果你看到以下错误：

```
ModuleNotFoundError: No module named 'distutils'
或
from distutils.version import LooseVersion
ImportError: cannot import name 'LooseVersion' from 'distutils.version'
```

这是因为 **Python 3.13 移除了 distutils 模块**，但 PySpark 仍然依赖它。

## 快速解决方案

### 方法 1: 自动修复（推荐）✓

运行修复脚本，自动安装依赖并应用补丁：

```bash
python fix_python313.py
```

### 方法 2: 手动修复

```bash
# 1. 安装必要的依赖
pip install setuptools packaging

# 2. 重新安装 requirements
pip install -r requirements.txt

# 3. 测试修复
python test_pyspark_windows.py
```

### 方法 3: 使用兼容性补丁

在你的 Python 代码开头添加：

```python
# 在导入 PySpark 之前
from pyspark_windows_compat import apply_all_patches
apply_all_patches()

# 然后正常导入 PySpark
from pyspark.sql import SparkSession
```

## 验证修复

运行以下命令确认修复成功：

```bash
# 测试 distutils
python -c "from distutils.version import LooseVersion; print('✓ distutils 可用')"

# 测试 PySpark
python -c "from pyspark.sql import SparkSession; print('✓ PySpark 可用')"

# 完整测试
python test_pyspark_windows.py
```

## 为什么会有这个问题？

| Python 版本 | distutils 状态 | PySpark 兼容性 |
|------------|---------------|---------------|
| 3.8 - 3.11 | ✓ 内置        | ✓ 完全兼容     |
| 3.12       | ⚠️ 已废弃     | ⚠️ 需要 setuptools |
| 3.13       | ✗ 已移除      | ✗ 需要补丁和 setuptools |

## 解决方案原理

我们的补丁使用以下策略：

1. **优先方案**: 使用 `setuptools._distutils` (setuptools 内置的 distutils 副本)
2. **备用方案**: 使用 `packaging.version` 作为替代

两种方案都可以提供 `LooseVersion` 功能。

## 长期建议

### 最佳实践 ✓

- **Python 版本**: 使用 Python 3.11 或 3.12
- **PySpark 版本**: 使用 PySpark 3.5.x
- **原因**: 这是最稳定、测试最充分的组合

### 如果必须使用 Python 3.13

确保始终安装：
```bash
pip install setuptools packaging
```

并在代码中应用补丁（已自动集成到 `app.py`）。

## 相关文档

- 详细修复指南: [PYSPARK_WINDOWS_FIX.md](PYSPARK_WINDOWS_FIX.md)
- 完整摘要: [PYSPARK_FIX_SUMMARY.md](PYSPARK_FIX_SUMMARY.md)
- 环境搭建: [ENVIRONMENT_SETUP.md](ENVIRONMENT_SETUP.md)

## 获取帮助

如果问题仍然存在：

1. 运行诊断: `python fix_python313.py`
2. 查看完整错误: `python test_pyspark_windows.py`
3. 检查安装: `pip list | grep -E "(pyspark|setuptools|packaging)"`

## 技术细节

补丁实现代码（已包含在 `pyspark_windows_compat.py`）：

```python
def apply_python313_patch():
    """为 Python 3.13 应用 distutils 兼容性补丁"""
    try:
        # 尝试使用 setuptools 的 distutils
        import setuptools._distutils as distutils
        sys.modules['distutils'] = distutils
        sys.modules['distutils.version'] = distutils.version
    except ImportError:
        # 回退到 packaging
        from packaging import version as pkg_version
        # 创建兼容层...
```

项目已自动集成此补丁，无需手动操作。

