# from ..data.raw_data import DataReady
# from __future__ import annotations
from typing import TYPE_CHECKING
import pandas as pd
# from stucture.data.raw_data import DataReady
from structure.data.raw_data import DataLoader, DataReader
from structure.domain.base import DatasetHandler

if TYPE_CHECKING:
    from structure.data.raw_data import DataLoader

class DM(DatasetHandler):
    # def __init__(self) -> None:
    #     self.dataready = None
    
    def get_dataset_name(self) -> str:
        return 'dm'
    
    def process(self, datareader: DataReader):
        # self.dataready = dataready
        # raw_data_store = datareader.prepare_raw_data(config_file= 'dm_rawdata_config.xlsx')
        # raw_data_store: dict[str, pd.DataFrame] = self.dataready.prepare_raw_data(config_file= 'dm_rawdata_config.xlsx')
        # spec_df = datareader.read_spec_df()
        # self.core_task(raw_data_store, spec_df)
         
        # raw_data:dict = {}
        # for table_id in ['dm1', 'dm2', 'dm3']:
        #     table_config = config_df[config_df['程序中数据集ID'] == table_id].iloc[0]

        # 1、加载配置文件
        config_df = datareader.load_config(config_file= 'dm_rawdata_config.xlsx')

        # 2、读取原始数据
        raw_data = self._load_raw_data(datareader= datareader, config_df= config_df)

        # 3、核心加工
        dm_table = self._process_core_table(raw_data= raw_data, config_df= config_df)

        # 读取规范数据
        # spec_df = datareader.read_spec_df()
        final_dm = self._arrange_output(dm_table)

        return final_dm
    
    def _process_core_table(self, raw_data: dict[str, pd.DataFrame], config_df: pd.DataFrame) -> pd.DataFrame:
        # final_dm_table
        all_dfs: list = []

        for table_id in config_df['程序中数据集ID'].unique():
            if table_id not in raw_data:
                continue
            
            df = raw_data[table_id].copy()
            
            # 获取 table_id 的配置
            table_config = config_df[config_df['程序中数据集ID'] == table_id]

            # 1、应用where条件
            df = self._apply_where_condition(df, table_config)
            
            # 2、重命名列
            df = self._rename_columns(df, table_config)
            
            all_dfs.append(df)
            
        merge_df = pd.concat(all_dfs, ignore_index= True)

        # 患者数据聚合
        patient_df = self._map_patient_data(merge_df)
        
        return pd.concat(all_dfs, ignore_index= True)
      

    def _map_patient_data(self, df: pd.DataFrame) -> pd.DataFrame:
        ''' 按患者聚合数据 '''
        # 确保列存在
        required_cols = ['subjid', 'sex', 'brthdtc', 'height', 'weight']
        for col in required_cols:
            if col not in df.columns:
                print(f'警告：缺少列: {col}')
                return pd.DataFrame()
        # 聚合
        result = df.groupby('subjid').agg({
            'sex': self._map_sex,
            'brthdtc': self._map_brthdtc,
            'height': self._map_height_weight,
            'weight': self._map_height_weight
        }).reset_index()
        
        result['bmi'] = self._calculate_bmi(result['weight'], result['height'])
        return result
        
    def _map_sex(self, series: pd.Series) -> str:
        mode_values = series.mode()
        if len(mode_values) == 1:
            return mode_values.iloc[0]
        else:
            return ''
    
    def _map_brthdtc(self, series: pd.Series) -> str:
        mode_values = series.mode()
        if len(mode_values) == 1:
            return mode_values.iloc[0]
        else:
            return min(series)
        
    def _map_height_weight(self, series: pd.Series) -> float:
        mode_values = series.mode()
        if len(mode_values) == 1:
            return mode_values.iloc[0]
        else:
            return series.mean()
        
        
    def _calculate_bmi(self, weight: pd.Series, height: pd.Series) -> pd.Series:
        ''' 计算BMI '''
        weight = pd.to_numeric(weight, errors= 'coerce')  
        height = pd.to_numeric(height, errors= 'coerce')/ 100
        bmi = weight / (height ** 2)
        return bmi
        
        

        


        
        
        
        
        
    # def run_dm_ds(self, dataready_instance: 'DataReady') -> None:
    #     self.dataready = dataready_instance
    #     raw_data_store: dict[str, pd.DataFrame] = self.dataready.prepare_raw_data(config_file= 'dm_rawdata_config.xlsx')
    #     self.core_task(raw_data_store)
        
    # def core_task(self, raw_data_store: dict[str, pd.DataFrame], spec_df:pd.DataFrame):
    #     # if self.dataready:
    #     #     spec_df = self.dataready.read_spec_df()
    #     dm1_df: pd.DataFrame = raw_data_store['dm1']
    #     dm2_df: pd.DataFrame = raw_data_store['dm2']
    #     dm3_df: pd.DataFrame = raw_data_store['dm3']
        
    
    # def map_sex(self):
    #     pass
    
    # def map_brthdtc(self):
    #     pass
    
    # def map_height_weight(self):
    #     pass
    
    # def calculate_bmi(self):
    #     pass