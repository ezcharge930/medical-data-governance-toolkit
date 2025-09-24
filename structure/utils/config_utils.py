
# import os
# import json
# from typing import Any
# import pandas as pd
# from .file_utils import FileUtils

# class ConfigUtils:
#     def __init__(self, fileutils: FileUtils) -> None:
#         self._config_dir = os.path.join(
#             os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
#             'config'
#         )
#         self.fileutils = fileutils
        
#     def get_supported_dataset(self) -> list[str]:
#         ''' #NOTE 获取支持的数据列表'''
#         try:
#             config = self._load_dataset_config()
#             return config.get('supported_datasets', ['dm'])
#         except Exception as e:
#             print(f'读取支持数据集配置失败,使用默认值：{e}')
#             return ['dm']
        
#     def get_default_datasets(self) -> list[str]:
#         """获取默认数据集列表"""
#         try:
#             config = self._load_dataset_config()
#             return config.get('default_datasets', ['dm'])
#         except Exception as e:
#             print(f"读取默认数据集配置失败，使用默认值: {e}")
#             return ['dm']  
        
#     def get_dataset_descriptions(self) -> dict[str, str]:
#         ''' #NOTE 获取数据集详细信息'''
#         try:
#             config = self._load_dataset_config()
#             return config.get('dateset_description', {})
#         except Exception as e:
#             print(f'读取数据集描述配置失败：{e}')
#             return {}
            
    
#     def _load_dataset_config(self) -> dict[str, Any]:
#         ''' #NOTE 加载数据集配置文件'''
#         config_path = os.path.join(self._config_dir, 'datasets.json')
        
#         if not os.path.exists(config_path):
#             # NOTE 若配置文件不存在,创建默认配置
#             self._create_default_dataset_config(config_path)
#         with open(config_path, 'r', encoding= 'utf-8') as f:
#             return json.load(f)
        
#     def _create_default_dataset_config(self, config_path: str):
#         ''' #NOTE 创建默认数据集配置文件'''
#         default_config = {
#             "supported_datasets": [
#                 "dm", "dg"
#             ],
#             "default_datasets": ["dm"],
#             "dataset_description": {
#                 "dm": "人口统计学",
#                 "dg": "诊断信息"
#             }
#         }
#         # 确保目录存在
#         os.makedirs(os.path.dirname(config_path), exist_ok= True)
        
#         # 写入配置文件
#         with open(config_path, 'w', encoding= 'utf-8') as f:
#             json.dump(default_config, f, ensure_ascii= False, indent= 2)
        
#         print(f'已创建默认数据集配置文件: {config_path}')
        
        
#     # @staticmethod
#     def get_whole_config(self, config_file: str = 'whole_config.xlsx') -> dict:
#         ''' #NOTE 获取完整配置'''
#         spec_path = self.fileutils.get_raw_data_path(config_file= config_file, path= 'config')
#         spec_df = pd.read_excel(spec_path, dtype= object, sheet_name= 0, keep_default_na= False)
#         return spec_df.iloc[0].to_dict()
#         # return {}
    
#     # @staticmethod
#     def get_exe_ls(self, config_file: str = 'whole_config.xlsx', target_sheet: str = '数据集执行控制') -> list[str]:
#         ''' #NOTE 获取要执行的数据集列表'''
#         # 这个可以读取另外一个配置文件,指定当前项目要执行哪些数据集
#         spec_path: str = self.fileutils.get_raw_data_path(config_file= config_file, path= 'config')
#         spec_df = pd.read_excel(spec_path, dtype= object, sheet_name= target_sheet, keep_default_na= False)
#         spec_df.fillna('', inplace= True)
#         dataset_col = '数据集代号'
#         execute_flag_col = '是否执行'
#         if dataset_col not in spec_df.columns or execute_flag_col not in spec_df.columns:
#             raise ValueError(f'配置表 {target_sheet} 缺少必要列: {dataset_col} 或 {execute_flag_col}')
#         enabled_datasets: list = spec_df[spec_df[execute_flag_col] == 'y'][dataset_col].tolist()
#         return enabled_datasets
