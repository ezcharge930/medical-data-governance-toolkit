from ..utils.file_utils import FileUtils
from ..utils.db_utils import DBUtils
from .raw_data import DataReady
from ..domain.dm import DM

class DataProcessor:
    def __init__(self, file_utils: FileUtils, db_utils: DBUtils, dataready:DataReady) -> None:
        self.file_utils = file_utils
        self.db_utils = db_utils
        self.dataready = dataready
        
    def run_dataset(self, dataset_name: str):
        ''' #NOTE 运行指定的数据集处理'''
        handlers = {
            'dm': self.run_dm,
            'dg': self.run_dg
        }
    
    def run_dm(self):
        dm = DM()
        dm.run_dm_ds(self.dataready)
        self.save_result()
        
    def run_sv(self):
        pass
    
    def run_dg(self):
        pass

        
    
    def save_result(self):
        self.file_utils.save_df_by_timestamp()