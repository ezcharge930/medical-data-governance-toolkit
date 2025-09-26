import json
import os
import pandas as pd
import pytest
from pathlib import Path

from src.utils.config_service import ConfigurationService


@pytest.fixture
def setup_test_config(tmp_path):
    ''' 创建临时测试配置文件 '''
    config_dir = tmp_path / 'config'
    config_dir.mkdir()
    
    # 1. 创建 whole_config.xlsx
    whole_data = {
        'key1': ['value1'],
        'key2': ['value2']
    }
    whole_df = pd.DataFrame(whole_data)
    exec_control_df = pd.DataFrame({
        '数据集代号': ['dm', 'log', 'user'],
        '是否执行': ['y', 'n', 'y']
    })
    
    calc_rule_df = pd.DataFrame({
        '数据集': ['dm', 'dm', 'log'],
        '输出标志': ['y', 'n', 'y'],
        '字段名': ['id', 'name', 'event']
    })
    
    whole_config_path = config_dir / 'whole_config.xlsx'

    with pd.ExcelWriter(whole_config_path, engine= 'openpyxl') as writer:
        whole_df.to_excel(writer, sheet_name = 'whole_config', index= False)
        exec_control_df.to_excel(writer, sheet_name= '数据集执行控制', index= False)
        calc_rule_df.to_excel(writer, sheet_name= '计算规则', index= False)

    # 2. 创建 dm_rawdata_config.xlsx
    dm_config = pd.DataFrame({
        'source_table': ['users'],
        'filter_condition': ['avtive = 1']
    })
    dm_config_path = config_dir / 'dm_rawdata_config.xlsx'
    dm_config.to_excel(dm_config_path, index= False)
    
    # 