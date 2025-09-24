import pandas as pd
import datetime

class OutputArranger:
    ''' 标准化输出整理器 '''
    def __init__(
        self,
        spec_columns: list[str],
        subjid_col: str = 'subjid',
        clean_chars: list[str]|None = None,
        add_timestamp: bool = True,
        timestamp_col: str = 'create_time',
        tiemstamp_value: datetime.date|None = None
    ) -> None:
        self.spec_columns = spec_columns
        self.subjid_col = subjid_col
        self.clean_chars = clean_chars or ['\n', '@']
        self.add_timestamp = add_timestamp
        self.timestamp_col = timestamp_col
        self.timestamp_value = tiemstamp_value or datetime.date.today()
        
    def arrange(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        ''' 按规范整理输出 '''
        # 1、按规范重排字段
        df = raw_df.reindex(columns= self.spec_columns, fill_value= '')

        # 2、清洗字符串
        clean_df = df.map(self._clean_value)
        
        # 3、过滤无效记录
        filter_df = self._filter_valid_records(clean_df)
        
        # 4、去重
        df = filter_df.drop_duplicates()
        
        # 5、添加时间戳
        if self.add_timestamp:
            df[self.timestamp_col] = self.timestamp_value
            
        return df
        
        
        
    def _clean_value(self, val):
        if isinstance(val, str):
            cleaned = val.strip()
            for char in self.clean_chars:
                cleaned = cleaned.replace(char, '')
            return cleaned
        return val
    
    def _filter_valid_records(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = (df[self.subjid_col] != '') & (pd.notnull(df[self.subjid_col]))
        result = df[mask].copy()
        return result