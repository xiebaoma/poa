"""
配置加载工具
用于加载和解析YAML配置文件
"""
import yaml
import os
from pathlib import Path


def load_config(config_path=None):
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径，如果为None，则使用默认路径
        
    Returns:
        dict: 配置字典
    """
    if config_path is None:
        # 获取项目根目录
        current_dir = Path(__file__).parent
        project_root = current_dir.parent.parent.parent.parent
        config_path = project_root / "src/main/resources/config/config.yaml"
    
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config


def get_data_paths(config=None):
    """
    获取数据路径配置
    
    Args:
        config: 配置字典，如果为None则自动加载
        
    Returns:
        dict: 包含raw_path, processed_path, results_path的字典
    """
    if config is None:
        config = load_config()
    
    data_config = config.get('data', {})
    project_root = Path(__file__).parent.parent.parent.parent
    
    return {
        'raw_path': project_root / data_config.get('raw_path', 'data/raw'),
        'processed_path': project_root / data_config.get('processed_path', 'data/processed'),
        'results_path': project_root / data_config.get('results_path', 'data/results')
    }

