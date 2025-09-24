from structure.utils.file_utils import FileUtils
from structure.utils.db_utils import DBUtils
from structure.data.raw_data import DataLoader
from structure.domain.dm import DatasetHandler
from structure.utils.config_service import ConfigurationService

import pandas as pd

class DataProcessor:
    def __init__(self, file_utils: FileUtils, db_utils: DBUtils, dataloader:DataLoader) -> None:
        self.file_utils = file_utils
        self.db_utils = db_utils
        self.dataloader = dataloader
        self._handler: dict[str, DatasetHandler] = {}
        
    def register_handler(self, handler: DatasetHandler):
        ''' #NOTE 注册一个数据集处理器'''
        name = handler.get_dataset_name().lower()
        self._handler[name] = handler
        
    def run_dataset(self, dataset_name: str, config_service: ConfigurationService):
        # ''' #NOTE 运行指定的数据集处理'''
        # handlers = {
        #     'dm': self.run_dm,
        #     'dg': self.run_dg
        # }
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

    
    # def run_dm(self):
    #     dm = DM()
    #     dm.run_dm_ds(self.dataready)
    #     self.save_result()
        
    # def run_sv(self):
    #     pass
    
    # def run_dg(self):
    #     pass

        
    
    def save_result(self, df:pd.DataFrame, dataset_name:str):
        self.file_utils.save_df_by_timestamp(df, prefix= dataset_name)