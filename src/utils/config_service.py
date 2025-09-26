import os
import json
import pandas as pd

class ConfigurationService:
    '''统一的配置服务,解耦配置加载与使用'''

    def __init__(self, config_dir: str|None = None) -> None:
        if config_dir is None:
            # 默认 config 目录: project_root/config
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.config_dir = os.path.join(project_root, 'config')
        else:
            self.config_dir = config_dir
            
    def load_json_config(self, filename: str) -> dict[str, dict]:
        ''' 加载 JSON 配置文件'''
        path = os.path.join(self.config_dir, filename)
        with open(path, 'r', encoding= 'utf-8') as f:
            return json.load(f)
        
    def load_excel_config(self, filename: str, sheet_name: str|int = 0) -> pd.DataFrame:
        ''' 加载 EXCEL 配置文件'''
        spec_path = os.path.join(self.config_dir, filename)
        spec_file = pd.read_excel(spec_path, sheet_name= sheet_name, dtype= object, keep_default_na= False).fillna('')
        return spec_file
    
    def get_whole_config(self) -> dict[str, dict]:
        ''' 获取 whole_config.xlsx 的第一行作为dict'''
        df = self.load_excel_config('whole_config.xlsx')
        return df.iloc[0].to_dict()

    def get_execution_datasets(self) -> list[str]:
        ''' 获取要执行的数据集列表 '''
        df = self.load_excel_config('whole_config.xlsx', sheet_name= '数据集执行控制')
        if '数据集代号' not in df.columns or '是否执行' not in df.columns:
            raise ValueError('配置表缺少必要列')
        return df[df['是否执行'] == 'y']['数据集代号'].tolist()
    
    def get_dataset_config(self, dataset_name: str) -> pd.DataFrame:
        ''' 获取特定数据集的配置(如:dm_raw_data_config.xlsx) '''
        filename = f'{dataset_name}_rawdata_config.xlsx'
        return self.load_excel_config(filename)
    
    def get_specification(self, domain: str) -> pd.DataFrame:
        ''' 读取数据说明书,并返回输出字段与顺序'''
        spec_path: pd.DataFrame = self.load_excel_config('whole_config.xlsx', sheet_name= 'whole_config')

        spec_df = pd.read_excel(spec_path, sheet_name= '计算规则', dtype= object, keep_default_na= False).fillna('')
        
        spec_df = spec_df.loc[(spec_df['数据集'] == domain) & (spec_df['输出标志'].str.lower() == 'y')]

        return spec_df
