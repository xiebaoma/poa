# 🚨 问题解决：系统找不到指定的路径

## 根本原因

**你的系统没有安装 Java！**

PySpark 依赖 Java 运行环境（JRE/JDK），没有 Java，Spark 无法启动。

## 快速解决方案

### 方法 1: 安装 Java（推荐）✓

#### Windows 用户：

1. **下载 Java 11 或 17**（推荐 17）
   - 官方下载：https://adoptium.net/
   - 选择：**Windows x64 .msi 安装包**

2. **安装 Java**
   - 双击下载的 `.msi` 文件
   - 安装过程中勾选 "Set JAVA_HOME variable"
   - 安装过程中勾选 "Add to PATH"

3. **验证安装**
   ```cmd
   # 重新打开 PowerShell 窗口
   java -version
   ```
   
   应该显示类似：
   ```
   openjdk version "17.0.x"
   ```

4. **重新运行程序**
   ```cmd
   python test_data_loading.py
   ```

### 方法 2: 使用便携版 Java（无需安装）

如果不想安装 Java，可以下载便携版：

1. 下载 ZIP 版本的 Java：https://adoptium.net/
2. 解压到某个目录，例如：`C:\Java\jdk-17`
3. 设置环境变量（临时）：
   ```cmd
   $env:JAVA_HOME = "C:\Java\jdk-17"
   $env:PATH = "C:\Java\jdk-17\bin;$env:PATH"
   ```
4. 验证：
   ```cmd
   java -version
   ```

## 为什么会出现这个错误？

1. **PySpark 依赖 Java**
   - PySpark 是 Apache Spark 的 Python 接口
   - Spark 核心是用 Scala/Java 编写的
   - 因此必须有 Java 才能运行

2. **错误信息不明确**
   - Windows 只显示"系统找不到指定的路径"
   - 实际上是找不到 Java 可执行文件
   - 完整错误信息被 Python 异常吞没了

## 安装后的验证步骤

```cmd
# 1. 验证 Java
java -version

# 2. 验证环境变量
echo $env:JAVA_HOME

# 3. 测试 PySpark
python test_pyspark_windows.py

# 4. 测试数据加载
python test_data_loading.py

# 5. 运行完整程序
python src/main/python/main/app.py --generate -n 100
```

## Java 版本选择

| Java 版本 | PySpark 3.5.x | PySpark 4.x | 推荐度 |
|-----------|---------------|-------------|--------|
| Java 8    | ✓ 完全支持    | ⚠️ 部分支持  | ⭐⭐⭐  |
| Java 11   | ✓ 完全支持    | ✓ 完全支持   | ⭐⭐⭐⭐⭐ |
| Java 17   | ✓ 完全支持    | ✓ 完全支持   | ⭐⭐⭐⭐⭐ |
| Java 21   | ⚠️ 可能有问题 | ⚠️ 可能有问题 | ⭐     |

**推荐：Java 11 或 Java 17**

## 常见问题

### Q: 我已经安装了 Java，为什么还报错？

**A:** 可能原因：
1. 没有重启 PowerShell/终端
2. 没有添加到 PATH 环境变量
3. JAVA_HOME 环境变量没有设置

**解决**：
```cmd
# 检查 PATH
echo $env:PATH

# 检查 JAVA_HOME
echo $env:JAVA_HOME

# 手动设置（如果为空）
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

### Q: 可以使用 OpenJDK 吗？

**A:** 可以！推荐使用 Eclipse Temurin (AdoptOpenJDK)，这是官方推荐的 OpenJDK 发行版。

### Q: 安装 Java 后需要重启电脑吗？

**A:** 不需要重启电脑，但**必须重新打开 PowerShell/终端窗口**，这样才能加载新的环境变量。

### Q: 我可以使用 Oracle JDK 吗？

**A:** 可以，但 Oracle JDK 用于商业用途需要许可证。推荐使用免费的 Eclipse Temurin (AdoptOpenJDK)。

## 自动化安装脚本（高级）

如果你想自动化安装，可以使用 Chocolatey（Windows 包管理器）：

```cmd
# 1. 安装 Chocolatey（如果还没有）
# 以管理员身份运行 PowerShell：
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# 2. 使用 Chocolatey 安装 Java
choco install temurin17 -y

# 3. 重新打开 PowerShell
```

## 完整的环境检查清单

在运行舆情分析系统前，确认：

- [ ] ✓ Java 已安装：`java -version`
- [ ] ✓ JAVA_HOME 已设置：`echo $env:JAVA_HOME`
- [ ] ✓ Python 已安装：`python --version`
- [ ] ✓ PySpark 已安装：`pip show pyspark`
- [ ] ✓ setuptools 已安装（Python 3.13）：`pip show setuptools`
- [ ] ✓ 数据目录存在：`ls data/raw`

## 总结

你遇到的"系统找不到指定的路径"错误**不是代码问题**，而是：

1. **缺少 Java 环境** ← 主要原因
2. Python 3.13 兼容性问题（已修复）
3. Windows 路径问题（已修复）

**立即行动：**
1. 访问 https://adoptium.net/
2. 下载并安装 Java 17
3. 重新打开 PowerShell
4. 运行 `java -version` 验证
5. 运行 `python test_data_loading.py` 测试

安装 Java 后，所有问题都将解决！🎉

