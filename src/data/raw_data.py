import pandas as pd 


from typing import Protocol


class DataReader(Protocol):
    def read_raw_table(self, table_name: str, columns: list[str]) -> pd.DataFrame:
        ...

class DataLoader:
    def __init__(self) -> None:
        ...
        
    def read_raw_table(self, table_name: str, columns: list[str]) -> pd.DataFrame:
        ...
