from ..utils.config_utils import ConfigUtils

class TaskManager():
    def __init__(self, config: ConfigUtils) -> None:
        self.config = config
    
    def run_ds_multi_process(self):
        tasks = self.config.get_exe_ls()
    
    def process_main_task_ds(self):
        pass
    
    def combine_bath_df(self):
        pass
    
