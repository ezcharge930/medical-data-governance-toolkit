import os
import pandas as pd
import datetime

class FileUtils:
    def __init__(self, project_name: str|None = None) -> None:
        self.project_name = project_name
        # NOTE structure/utils
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        # NOTE structure
        self.structure_dir = os.path.dirname(self.current_dir)
    
    # NOTE 目前的项目暂时不需要去运行多个项目,就先基于本项目目录开始
    def get_raw_data_path(self, config_file: str | None, path: str) -> str:
        '''
        获取原始数据文件路径
        
        Args:
            config_file: 配置文件名
            path: 相对于stucture目录的路径
            
        Returns:
            完整的文件路径
        '''
        print(f"DEBUG: config_file={config_file}, path={path}")
        print(f"DEBUG: current_dir={self.current_dir}")
        print(f"DEBUG: structure_dir={self.structure_dir}")
        # 特殊处理config路径 - 指向stucture/config
        if path and 'config' in path.lower():
            # NOTE 构建 stucture/config 路径
            config_dir = os.path.join(self.structure_dir, 'config')
        else:
            # 其他情况，基于stucture目录
            if path:
                if not os.path.isabs(path):
                    config_dir = os.path.join(self.structure_dir, path)
                else:
                    config_dir = path
            else:
                config_dir = self.structure_dir
        
        print(f"DEBUG: config_dir={config_dir}")
        
        # 确保目录存在
        os.makedirs(config_dir, exist_ok=True)
        
        if config_file:
            # NOTE structure/config/xxxxx
            datafile = os.path.join(config_dir, config_file)
        else:
            datafile = config_dir
            
        print(f"DEBUG: 最终返回路径={datafile}")
        return datafile
    
    def get_all_raw_data_path(self):
        pass
    
    def read_csv_to_pd(self):
        pass
    
    def save_df_by_timestamp(self, df: pd.DataFrame, prefix: str = 'output'):
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{prefix}_{timestamp}.csv'
        filepath = os.path.join(self.structure_dir, 'output', filename)
        os.makedirs(os.path.dirname(filepath), exist_ok= True)
        df.to_csv(filepath, index= False,  encoding= 'utf-8')
        print(f'已保存: {filepath}')
    