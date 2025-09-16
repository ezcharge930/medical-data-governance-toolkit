import os
import sys
import logging
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from src.utils.excel_to_yaml import excel_to_yaml


def generate_mapping() -> None:
    ''' 批量生成映射配置文件'''
    design_dir = Path('docs/design')
    output_dir = Path('config/mapping')

    output_dir.mkdir(parents=True, exist_ok=True)

    success_count = 0
    error_count = 0


    for excel_file in design_dir.glob('*.xlsx'):
        try:

            output_file = output_dir / f'{excel_file.stem}.yaml'

            excel_to_yaml(
                excel_path=str(excel_file),
                yaml_path=str(output_file)
            )
            logging.info(f'Generated mapping file: {output_file}')
            success_count += 1
        except Exception as e:
            logging.error(f'Error processing {excel_file}: {e}')
            error_count += 1

    logging.info(f'Processing complete: {success_count} succeeded, {error_count} failed')
    

if __name__ == '__main__':
    generate_mapping()