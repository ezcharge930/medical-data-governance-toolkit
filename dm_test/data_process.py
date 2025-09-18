class DM:
    def run_dm_ds(self):
        self.core_task()
        
    def core_task(self):
        pass
    
    def map_sex(self):
        pass
    
    def map_brthdtc(self):
        pass
    
    def map_height_weight(self):
        pass
    
    def calculate_bmi(self):
        pass
        
        

class DataProcessor:
    def __init__(self) -> None:
        self.file_utils = FileUtils()
        self.db_utils = DBUtils()
    
    def prepare_raw_data(self):
        raw_paths = self.file_utils.get_raw_data_path()
    
    def run_dm(self):
        dm = DM()
        dm.run_dm_ds()
        self.save_result()
    
    def core_task(self):
        pass
    
    def save_result(self):
        self.file_utils.save_df_by_timestamp()
    
class TaskManager():
    def __init__(self) -> None:
        self.config = ConfigUtils()
    
    def run_ds_multi_process(self):
        tasks = self.config.get_exe_ls()
    
    def process_main_task_ds(self):
        pass
    
    def combine_bath_df(self):
        pass
    
class FileUtils:
    def get_raw_data_path(self):
        pass
    
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
    
class MainApp:
    def run_project(self):
        data_processor = DataProcessor()
        data_processor.run_dm()

    def run_df_process(self):
        taskmanager = TaskManager()
        taskmanager.run_ds_multi_process()

class ProcessManager:
    def main(self):
        app = MainApp()
        app.run_project()

import getopt
import sys

def command_line():
    project_name = None
    para = {}
    opts, args = getopt.getopt(
        sys.argv[1:],
        shortopts= 'hcaeqtbmrs',
        longopts= ["project=",'save_resolved_query','generate_query_work','export_query_report'])
    for opt, arg in opts
    
if __name__ == '__main__':
    pass