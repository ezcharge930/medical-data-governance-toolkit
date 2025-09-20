class FileUtils:
    # def __init__(self) -> None:
    #     self.project_name = parse_command_line().project
    
    # NOTE 目前的项目暂时不需要去运行多个项目,就先基于本项目目录开始
    def get_raw_data_path(self, config_file: str | None, path: str) -> str:
        datafile = '%s%s' %(path, config_file)
        return '0'
    
    def get_all_raw_data_path(self):
        pass
    
    def read_csv_to_pd(self):
        pass
    
    def save_df_by_timestamp(self):
        pass
    
class DBUtils:
    def create_model_class(self):
        pass
    
    def execute_raw_data_import_sqlite(self):
        pass
    
    def read_spec_df(self):
        pass
    
    def arrange_out_df(self):
        pass
    
class ConfigUtils:
    @staticmethod
    def get_whole_config():
        pass
    
    @staticmethod
    def get_exe_ls():
        pass
