from abc import ABC, abstractmethod
from structure.data.raw_data import DataLoader,DataReader
from structure.utils.config_service import ConfigurationService
import pandas as pd

class DatasetHandler(ABC):
    @abstractmethod
    def process(self, dataloader: DataLoader, config_service: ConfigurationService) -> pd.DataFrame:
        ''' 处理数据集,接受配置服务以便获取全局或者数据集专属配置 '''
        ...
    
    @abstractmethod
    def get_dataset_name(self) -> str:
        ''' 返回支持的数据集名,如'dm', 'sv'等'''
        ...
    
    def _process_core_table(self, raw_data: dict[str, pd.DataFrame], config_df: pd.DataFrame) -> pd.DataFrame:
        ...
    
    
    def _load_raw_data(self, datareader: DataReader, config_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        raw_data: dict[str, pd.DataFrame] = {}
        # 获取所有唯一的"程序中数据集ID"
        for table_id in config_df['程序中数据集ID'].unique():
            # 找到对应 table_id 的配置
            table_config = config_df[config_df['程序中数据集ID'] == table_id]
            source_table = table_config['来源表']
            source_columns = table_config['来源表变量'].tolist()
            raw_data[table_id] = datareader.read_raw_table(source_table, source_columns)
        return raw_data  

            
    def _apply_where_condition(self, df: pd.DataFrame, config: pd.DataFrame) -> pd.DataFrame:
        ''' 应用 where 条件'''
        for _, row in config.iterrows():
            if row['where语句']:
                # 替换 {$} 为实际列名
                condition = row['where语句'].replace('{$}', row['来源表变量'])
                try:
                    df = df.query(condition)
                except Exception as e:
                    print(f'where 条件错误:{condition}, 错误: {e}')
        return df
                    
                    
    def _rename_columns(self, df: pd.DataFrame, config: pd.DataFrame) -> pd.DataFrame:
        ''' 重命名列 '''
        rename_map: dict[str, str] = {}
        for _, row in config.iterrows():
            if row['重命名变量']:
                rename_map[row['来源表变量']] = row['重命名变量']
        return df.rename(columns= rename_map)
    
    # def _arrange_output(self, dm_table: pd.DataFrame, spec_df: pd.DataFrame) -> pd.DataFrame:
    #     # TODO: 根据 spec_df 调整输出列顺序、格式等
    #     ...