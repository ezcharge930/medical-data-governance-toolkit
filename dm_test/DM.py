from DataManager import DataReady
import pandas as pd

class DM:
    def __init__(self, dataready: DataReady) -> None:
        self.dataready = dataready
    
    def run_dm_ds(self):
        raw_data_store: dict[str, pd.DataFrame] = self.dataready.prepare_raw_data(config_file= 'dm_rawdata_config.xlsx')
        self.core_task(raw_data_store)
        
    def core_task(self, raw_data_store: dict[str, pd.DataFrame]):
        spec_df = self.dataready.read_spec_df()
        dm1_df: pd.DataFrame = raw_data_store['dm1']
        dm2_df: pd.DataFrame = raw_data_store['dm2']
        dm3_df: pd.DataFrame = raw_data_store['dm3']
        pass
    
    def map_sex(self):
        pass
    
    def map_brthdtc(self):
        pass
    
    def map_height_weight(self):
        pass
    
    def calculate_bmi(self):
        pass