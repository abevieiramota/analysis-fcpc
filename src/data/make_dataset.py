# -*- coding: utf-8 -*-
import os
import click
import logging
from dotenv import find_dotenv, load_dotenv
import pandas as pd

read_cfg = {'decimal': ',',
            'thousands': '.', 
            'parse_dates': [4],
            'date_parser': lambda date: pd.datetime.strptime(date, '%d/%m/%Y')
           }

columns_rename = {'TIPO DE PAGAMENTO': 'tipo_pagamento',
                  'VALOR (R$)': 'valor',
                  'CPF': 'cpf',
                  'NOME': 'nome',
                  'PROJETO': 'projeto',
                  'DATA': 'data'}




@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    filenames = os.listdir(input_filepath)

    df = pd.DataFrame()

    for filename in filenames:
        
        df_ = pd.read_csv(os.path.join(input_filepath, filename), **read_cfg)
        df_['filename'] = filename
        
        df = df.append(df_, ignore_index=True)
        
    df.rename(columns=columns_rename, inplace=True)

    df.to_csv(os.path.join(output_filepath, 'fisica.csv'), index=False, encoding='utf-8')




if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
