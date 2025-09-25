from src.utils.config_service import ConfigurationService

class TaskManager():
    def __init__(self, config_service: ConfigurationService) -> None:
        self.config_service = config_service
    
    def run_ds_multi_process(self):
        ...
    
    def process_main_task_ds(self):
        pass
    
    def combine_bath_df(self):
        pass
    
