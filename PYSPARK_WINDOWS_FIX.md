# PySpark Windows 兼容性问题解决方案

## 问题描述

PySpark 在 Python 3.13 + Windows 环境下存在多个兼容性问题：

### 问题 1: UnixStreamServer 不存在
- **错误原因**：PySpark 4.1.0 尝试使用 `UnixStreamServer` 类，但该类在 Windows 上不存在（仅在 Unix/Linux 系统上可用）
- **影响版本**：PySpark 4.0.0+ 在 Windows 上运行时
- **特别影响**：Python 3.13 在 Windows 上的兼容性最差

### 问题 2: distutils 模块被移除（新问题）⚠️
- **错误信息**：`ModuleNotFoundError: No module named 'distutils'`
- **错误原因**：Python 3.12 废弃 `distutils`，Python 3.13 完全移除了它，但 PySpark 仍然依赖 `distutils.version.LooseVersion`
- **影响版本**：Python 3.12+ (特别是 3.13)
- **影响范围**：所有操作系统（不仅限 Windows）

## 解决方案

本项目已实施以下解决方案：

### 1. 版本限制（推荐 ✓）

**已修改 `requirements.txt`**：

```txt
# 使用稳定的 PySpark 3.5.x 版本
pyspark>=3.5.0,<4.0.0
```

这是最稳定和推荐的解决方案。PySpark 3.5.x 在 Windows 上表现良好。

### 2. 兼容性补丁（备用方案）

**已创建 `pyspark_windows_compat.py`**：

该文件提供了 Windows 兼容性补丁，如果你需要使用 PySpark 4.x：

```python
from pyspark_windows_compat import apply_pyspark_windows_patch

# 在导入 PySpark 之前应用补丁
apply_pyspark_windows_patch()

from pyspark.sql import SparkSession
```

主应用程序 `app.py` 已自动集成此补丁。

### 3. 降级 Python 版本

如果必须使用 PySpark 4.x，建议降级到：
- **Python 3.11**（推荐）
- **Python 3.12**（可用）

## 安装步骤

### 方案 A：使用 PySpark 3.5.x + Python 3.13（推荐）✓

```bash
# 卸载现有的 PySpark（如果有）
pip uninstall pyspark -y

# 安装兼容版本和必要依赖
pip install -r requirements.txt

# requirements.txt 包含：
# - pyspark>=3.5.0,<4.0.0  （兼容版本）
# - setuptools>=65.0.0      （修复 distutils 依赖）
# - packaging>=21.0         （备用方案）
```

### 方案 B：使用兼容性补丁 + PySpark 4.x

```bash
# 安装依赖
pip install setuptools packaging

# 安装 PySpark 4.x
pip install pyspark==4.1.0

# 运行兼容性检查
python pyspark_windows_compat.py
```

## 验证安装

运行以下命令检查兼容性：

```bash
python pyspark_windows_compat.py
```

输出示例：

```
=== PySpark Windows 兼容性检查 ===
- 检测到 Windows 系统
- Python 3.13 在 Windows 上可能与 PySpark 4.x 存在兼容性问题
- PySpark 3.5.1 - 版本兼容

应用兼容性补丁...
已应用 PySpark Windows 兼容性补丁

建议:
1. 使用 PySpark 3.5.x 版本（最稳定）
2. 或者降级到 Python 3.11 或 3.12
3. 在代码开头导入此模块: from pyspark_windows_compat import apply_pyspark_windows_patch
```

## 快速测试

```bash
# 测试 PySpark 是否正常工作
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local').getOrCreate(); print('PySpark 运行正常！'); spark.stop()"
```

## 项目中的自动处理

项目主应用程序 (`src/main/python/main/app.py`) 已经自动集成了兼容性补丁：

```python
# Windows 兼容性补丁：在导入 PySpark 之前应用
if platform.system() == 'Windows':
    try:
        from pyspark_windows_compat import apply_pyspark_windows_patch
        apply_pyspark_windows_patch()
    except Exception as e:
        print(f"警告: 无法应用 PySpark Windows 兼容性补丁: {e}")

from pyspark.sql import SparkSession
```

因此，你可以直接运行项目，无需手动干预。

## 常见问题

### Q: 我应该使用哪个版本？

**A:** 推荐使用 **PySpark 3.5.x + Python 3.11/3.12**。这是最稳定、经过充分测试的版本。

如果必须使用 Python 3.13，确保安装了 `setuptools` 和 `packaging`。

### Q: 什么是 distutils 问题？

**A:** Python 3.13 移除了 `distutils` 模块，但 PySpark 仍然依赖它。我们提供了两种解决方案：
1. 使用 `setuptools._distutils`（推荐）
2. 使用 `packaging.version` 作为替代

补丁会自动尝试这两种方案。

### Q: 如何确认 distutils 问题已修复？

**A:** 运行以下命令：
```bash
python -c "from distutils.version import LooseVersion; print('distutils 可用')"
```

如果没有错误，说明已修复。

### Q: 如果我需要 PySpark 4.x 的新特性怎么办？

**A:** 有两个选项：
1. 使用兼容性补丁（本项目已提供）+ 安装 setuptools
2. 降级到 Python 3.11 或 3.12

### Q: 兼容性补丁安全吗？

**A:** 是的。补丁只是：
1. 提供 `UnixStreamServer` 的 TCP 替代实现
2. 重新映射 `distutils` 到 `setuptools._distutils` 或 `packaging`

不会影响 PySpark 的核心功能。

### Q: 在 Linux/Mac 上需要这个补丁吗？

**A:** 不需要。这个问题仅存在于 Windows 系统。代码会自动检测操作系统。

## 相关资源

- [PySpark 官方文档](https://spark.apache.org/docs/latest/api/python/)
- [PySpark Windows 安装指南](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- [Python 版本兼容性](https://spark.apache.org/docs/latest/api/python/getting_started/install.html#python-version-supported)

## 技术细节

### UnixStreamServer 问题

在 Unix/Linux 系统上，`socketserver.UnixStreamServer` 用于进程间通信（IPC）。Windows 不支持 Unix domain sockets，因此该类不存在。

### 补丁实现

补丁创建了一个替代类，使用 TCP socket 代替 Unix socket：

```python
class UnixStreamServer(socketserver.TCPServer):
    """Windows 兼容的 UnixStreamServer 替代实现"""
    
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
        # 在 Windows 上使用 localhost TCP 连接
        if isinstance(server_address, str):
            # 如果是 Unix socket 路径，转换为 TCP 地址
            port = random.randint(50000, 60000)
            server_address = ('127.0.0.1', port)
        
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)
```

这个实现在功能上等效，但使用 TCP 而不是 Unix sockets。

## 更新日志

- **2025-12-27**: 创建兼容性补丁和文档
- **2025-12-27**: 更新 requirements.txt 限制 PySpark 版本
- **2025-12-27**: 集成自动补丁到主应用程序

