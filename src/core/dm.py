
from pathlib import Path
import yaml
import pandas as pd

class DM:
    def __init__(self, config_path):
        self.config_path= Path(config_path)
        self.config = None
        self.source_data = {}

    def load_config(self):
        with open(self.config_path, 'r', encoding='utf-8') as file:
            self.config = yaml.safe_load(file)
        return True
    
    def get_subjid_source(self):
        if not self.config:
            return []
        
        for field_name, field_config in self.config['fields'].items():
            if field_name == 'subjid':
                sources: list[dict[str, str]] = []
                for source in field_config['source_table']:
                    sources.append({
                        'table': source['source_table'],
                        'field': source['source_field']
                    })
                return sources
        return []

    
    
    
if __name__ == '__main__':
    pass