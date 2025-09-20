from Utils import FileUtils, DBUtils, ConfigUtils
from DataManager import DataReady, DataProcessor, TaskManager

class MainApp:
    @staticmethod
    def run_project():
        file_utils = FileUtils()
        db_utils = DBUtils()
        dataready = DataReady(file_utils, db_utils)
        config_utils = ConfigUtils()
        data_processor = DataProcessor(file_utils, db_utils, dataready)
        data_processor.run_dm()

    def run_df_process(self):
        config_utils = ConfigUtils()
        taskmanager = TaskManager(config_utils)
        taskmanager.run_ds_multi_process()