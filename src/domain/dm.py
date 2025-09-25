
from typing import TYPE_CHECKING
import pandas as pd

from src.data.raw_data import DataReader
from src.domain.base import DatasetHandler
from src.utils.config_service import ConfigurationService
from src.core.output_arrange import OutputArranger


class DM(DatasetHandler):
    
    def get_dataset_name(self) -> str:
        return 'dm'
    
    def process(self, datareader: DataReader, config_service: ConfigurationService):
       
        whole_config = config_service.get_whole_config()
        print(f'全局配置: {whole_config}')

        config_df = config_service.get_dataset_config('dm')

        # 2、读取原始数据
        raw_data = self._load_raw_data(datareader, config_df)

        # 3、核心加工
        dm_table = self._process_core_table(raw_data, config_df)

        # 4、读取数据说明书
        spec_df: pd.DataFrame = config_service.get_specification(domain= 'DM')
        
        # 创建整理器
        arranger = OutputArranger(
            spec_columns= spec_df['变量名'].tolist(),
            subjid_col= 'subjid',
            clean_chars= ['\n', '@', '\r'],
            add_timestamp= True
        )
        
        final_dm = arranger.arrange(dm_table)

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
        
        return patient_df
      

    def _map_patient_data(self, df: pd.DataFrame) -> pd.DataFrame:
        ''' 按患者聚合数据 '''
        # 确保列存在
        required_cols: list = ['subjid', 'sex', 'brthdtc', 'height', 'weight']
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
        
