import pandas as pd
import json
import sys
import os
import zipfile

import logging

logging.basicConfig(
  level=logging.DEBUG, 
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
  datefmt='%Y-%m-%d %H:%M',
  handlers=[logging.FileHandler('ETL.log', 'w'), logging.StreamHandler(sys.stdout)])

logger = logging.getLogger('ETL')

input_dir = './data'  # input data directory
output_dir = './output'   # output data directory


def extract(input_file: str) -> pd.DataFrame:
    ''' Extract data from csv file and return it as pd.DataFrame

      Args:
          input_file: The input file name

      Returns:
          data: pd.DataFrame data
    '''

    logger.info(f'Extracting data from {input_file} ...')

    data = pd.read_csv(os.path.join(input_dir, input_file))

    logger.info(f'Extracted {len(data)} data.')

    return data


def transform(data: pd.DataFrame, fix_file: str) -> pd.DataFrame:
    ''' Transform data with fix_file

    Args:
        data: the data to be transformed
        fix_file: use the info in fix_file to transform data
    
    Returns:
        data: transformed data

    '''    

    def fix_unblended_rate(to_fix: pd.DataFrame, unblended_rate: float) -> pd.DataFrame:
        
        # fix unblended cost
        to_fix['lineItem/UnblendedCost'] = to_fix.apply(
          lambda x: x['lineItem/UsageAmount'] * unblended_rate, axis=1)

        # fix line item description
        to_fix['lineItem/LineItemDescription'] = to_fix.apply(
          lambda x: x['lineItem/LineItemDescription'].replace(str(x['lineItem/UnblendedRate']), str(unblended_rate)), axis=1)

        # fix unblended rate
        to_fix['lineItem/UnblendedRate'] = unblended_rate

        return to_fix


    logger.info(f'Transform data ...')

    fix_jsons = []
    with open(os.path.join(input_dir, fix_file), 'r', encoding='utf-8') as file:
        fix_jsons = json.load(file)

    n_fix = 0
    for fix_json in fix_jsons:

        usage_account_id, usage_type, unblended_rate = fix_json.values()

        to_fix = data[
          (data['lineItem/UsageAccountId'] == int(usage_account_id)) & 
          (data['lineItem/UsageType'] == usage_type) & 
          (data['product/ProductName'] == 'Amazon CloudFront') & 
          (data['lineItem/LineItemType'] == 'Usage')]

        if not to_fix.empty:
            to_fix = fix_unblended_rate(to_fix, unblended_rate)
            data[
              (data['lineItem/UsageAccountId'] == int(usage_account_id)) & 
              (data['lineItem/UsageType'] == usage_type) & 
              (data['product/ProductName'] == 'Amazon CloudFront') & 
              (data['lineItem/LineItemType'] == 'Usage')] = to_fix

            n_fix += len(to_fix)
        else:
            logger.warning(f'No data to be fixed. fix_json: {fix_json}')
    
    logger.info(f'Transformed {n_fix} data.')
    return data
    
  
def load(data: pd.DataFrame) -> None: 
    ''' Load data into files

    Args:
        data: the data to be loaded
    
    Returns:
        None

    '''

    logger.info('Loading data into ./output ...')
    
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    for i, _id in enumerate(data['lineItem/UsageAccountId'].unique()):
        
        if i != 0 and i % 5 == 0:
            logger.info(f'Loaded {i} files.')

        zip_file = os.path.join(output_dir, f'{_id}.zip')
        csv_file = f'{_id}.csv'

        data[data['lineItem/UsageAccountId'] == _id].to_csv(csv_file, index=False)
        zf = zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED)
        zf.write(csv_file)

        zf.close()
        os.remove(csv_file)

    logger.info(f'Totally loaded {i} files.')


if __name__ == '__main__':

    input_file = 'output.csv'
    fix_file = 'fix.json'

    logger.info('Start ETL process')

    data = extract(input_file)
    data = transform(data, fix_file)

    load(data)

