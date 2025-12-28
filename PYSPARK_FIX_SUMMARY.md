# PySpark Windows 兼容性修复 - 修改摘要

## 问题

PySpark 4.1.0 在 Python 3.13 + Windows 上存在兼容性问题：
- 错误：`AttributeError: module 'socketserver' has no attribute 'UnixStreamServer'`
- 原因：`UnixStreamServer` 类在 Windows 上不存在（仅限 Unix/Linux）

## 解决方案

本项目已实施以下修复措施：

### 1. 修改的文件

#### `requirements.txt`
- **修改**：限制 PySpark 版本为 3.5.x
- **从**: `pyspark>=3.5.0`
- **到**: `pyspark>=3.5.0,<4.0.0`
- **原因**：PySpark 3.5.x 在 Windows 上完全兼容

#### `src/main/python/main/app.py`
- **修改**：在导入 PySpark 之前自动应用 Windows 兼容性补丁
- **添加**：
  - Windows 系统检测
  - 自动加载和应用兼容性补丁
  - 失败时的友好警告信息

#### `ENVIRONMENT_SETUP.md`
- **修改**：更新软件要求说明
- **添加**：
  - Python 3.13 兼容性警告
  - PySpark 版本限制说明
  - Windows 故障排除部分
  - 指向详细修复文档的链接

### 2. 新增的文件

#### `pyspark_windows_compat.py`
- **用途**：Windows 兼容性补丁模块
- **功能**：
  - 检测 Windows 系统
  - 为缺失的 `UnixStreamServer` 提供 TCP 替代实现
  - 环境兼容性检查工具
  - 可独立运行以检查系统

#### `PYSPARK_WINDOWS_FIX.md`
- **用途**：详细的问题说明和解决方案文档
- **内容**：
  - 问题描述和技术细节
  - 多种解决方案（版本限制、补丁、Python 降级）
  - 安装和验证步骤
  - 常见问题解答
  - 技术实现细节

#### `install_windows.bat`
- **用途**：Windows 自动安装脚本
- **功能**：
  - 自动检查 Python 和 Java 版本
  - Python 3.13 警告提示
  - 创建虚拟环境
  - 安装所有依赖
  - 运行兼容性检查

#### `test_pyspark_windows.py`
- **用途**：PySpark Windows 兼容性测试脚本
- **功能**：
  - 测试 PySpark 导入
  - 测试 SparkSession 创建
  - 测试基本 DataFrame 操作
  - 显示环境信息
  - 提供故障排除建议

#### `PYSPARK_FIX_SUMMARY.md`（本文件）
- **用途**：修改摘要文档

## 使用说明

### 方案 A：自动安装（推荐）

**Windows 用户**：
```cmd
install_windows.bat
```

### 方案 B：手动安装

1. **卸载现有 PySpark**：
```bash
pip uninstall pyspark -y
```

2. **安装兼容版本**：
```bash
pip install -r requirements.txt
```

3. **运行兼容性测试**：
```bash
python test_pyspark_windows.py
```

### 方案 C：使用 PySpark 4.x（高级用户）

如果你需要 PySpark 4.x 的特性：

1. **安装 PySpark 4.x**：
```bash
pip install pyspark==4.1.0
```

2. **运行兼容性补丁**：
```bash
python pyspark_windows_compat.py
```

3. **测试**：
```bash
python test_pyspark_windows.py
```

注意：补丁已自动集成到 `app.py` 中。

## 验证

运行以下命令验证修复：

```bash
# 测试 PySpark 基本功能
python test_pyspark_windows.py

# 测试完整系统
python scripts/quick_start.py

# 或运行主程序
python src/main/python/main/app.py --generate -n 100
```

## 兼容性矩阵

| 操作系统 | Python 版本 | PySpark 3.5.x | PySpark 4.x | 补丁需求 |
|---------|------------|--------------|------------|---------|
| Windows | 3.8 - 3.12 | ✓ 完全兼容    | ⚠️ 需要补丁  | 可选     |
| Windows | 3.13       | ✓ 完全兼容    | ✗ 不兼容    | 必需     |
| Linux   | 3.8+       | ✓ 完全兼容    | ✓ 完全兼容  | 不需要   |
| macOS   | 3.8+       | ✓ 完全兼容    | ✓ 完全兼容  | 不需要   |

## 推荐配置

| 用途 | 推荐配置 |
|-----|---------|
| **生产环境** | Python 3.11 + PySpark 3.5.x |
| **开发环境** | Python 3.11/3.12 + PySpark 3.5.x |
| **Windows** | Python 3.11 + PySpark 3.5.x（本项目默认）|
| **Linux/macOS** | Python 3.9+ + PySpark 3.5.x 或 4.x |

## 故障排除

### 问题：导入错误
```
AttributeError: module 'socketserver' has no attribute 'UnixStreamServer'
```

**解决**：
1. 检查 PySpark 版本：`pip show pyspark`
2. 如果是 4.x，降级：`pip install 'pyspark>=3.5.0,<4.0.0'`
3. 或运行补丁：`python pyspark_windows_compat.py`

### 问题：Java 未找到
```
Exception: Java gateway process exited before sending its port number
```

**解决**：
1. 安装 Java 8 或 11：https://adoptium.net/
2. 设置 JAVA_HOME 环境变量
3. 验证：`java -version`

### 问题：内存不足
```
OutOfMemoryError
```

**解决**：
修改 `src/main/resources/config/config.yaml`：
```yaml
spark:
  executor_memory: "1g"  # 降低内存
  driver_memory: "512m"
```

## 相关文档

- **详细修复指南**：[PYSPARK_WINDOWS_FIX.md](PYSPARK_WINDOWS_FIX.md)
- **环境搭建**：[ENVIRONMENT_SETUP.md](ENVIRONMENT_SETUP.md)
- **快速开始**：运行 `install_windows.bat`

## 技术支持

如果遇到问题：
1. 查看 [PYSPARK_WINDOWS_FIX.md](PYSPARK_WINDOWS_FIX.md)
2. 运行 `python test_pyspark_windows.py` 进行诊断
3. 检查环境：`python pyspark_windows_compat.py`

## 更新日志

**2025-12-27**：
- ✓ 限制 PySpark 版本为 3.5.x
- ✓ 添加 Windows 兼容性补丁
- ✓ 更新文档和安装脚本
- ✓ 集成自动补丁到主程序
- ✓ 添加测试脚本

## 贡献者

- 初始修复：2025-12-27
- 修复范围：Windows + Python 3.13 兼容性

---

**注意**：此修复确保项目在所有 Python 3.8+ 和 Windows 10/11 上正常运行。

