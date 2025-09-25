from src.utils.file_utils import FileUtils
from src.utils.config_service import ConfigurationService


from src.data.raw_data import DataLoader
from src.domain.dm import DatasetHandler


import pandas as pd

class DataProcessor:
    def __init__(self,  dataloader:DataLoader) -> None:
        self.dataloader = dataloader
        self._handler: dict[str, DatasetHandler] = {}
        
    def register_handler(self, handler: DatasetHandler):
        ''' #NOTE 注册一个数据集处理器'''
        name = handler.get_dataset_name().lower()
        self._handler[name] = handler
        
    def run_dataset(self, dataset_name: str, config_service: ConfigurationService):
        # ''' #NOTE 运行指定的数据集处理'''
        dataset_name = dataset_name.lower()
        handler = self._handler.get(dataset_name)
        
        if not handler:
            raise ValueError(f'不支持的数据集: {dataset_name}. 已注册: {list(self._handler.keys())}')
        
        try:
            print(f'开始处理 {dataset_name.upper()} 数据集')
            result_df: pd.DataFrame = handler.process(self.dataloader, config_service)
            self.save_result(result_df, dataset_name)
            print(f'{dataset_name.upper()} 处理完成')
        except Exception as e:
            print(f'{dataset_name.upper()} 处理失败: {e}')
            raise

    def save_result(self, df:pd.DataFrame, dataset_name:str):
        file_utils = FileUtils()
        file_utils.save_df_by_timestamp(df, prefix= dataset_name)