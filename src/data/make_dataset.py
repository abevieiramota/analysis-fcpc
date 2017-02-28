# -*- coding: utf-8 -*-
import os
import click
import logging
import pandas as pd

PF_READ_CFG = {'decimal': ',',
            'thousands': '.', 
            'parse_dates': [4],
            'date_parser': lambda date: pd.datetime.strptime(date, '%d/%m/%Y')
           }

PF_COLUMNS_RENAME = {'TIPO DE PAGAMENTO': 'tipo_pagamento',
                  'VALOR (R$)': 'valor',
                  'CPF': 'cpf',
                  'NOME': 'nome',
                  'PROJETO': 'projeto',
                  'DATA': 'data'}

PF_DIR = r"..\..\data\raw\fcpc\pagamento pessoa fisica"
PF_OUT_FILEPATH = r"..\..\data\processed\fcpc\fisica.csv"

PJ_READ_CFG = {'decimal': ',',
            'thousands': '.', 
            'parse_dates': [3],
            'date_parser': lambda date: pd.datetime.strptime(date, '%d/%m/%Y'),
            'quotechar': '"',
            'encoding': 'utf-8-sig'
           }

PJ_COLUMNS_RENAME = {'VALOR (R$)': 'valor',
                  'CNPJ': 'cnpj',
                  'NOME': 'nome',
                  'PROJETO': 'projeto',
                  'DATA': 'data'}

PJ_DIR = r"..\..\data\raw\fcpc\pagamento pessoa juridica"
PJ_OUT_FILEPATH = r"..\..\data\processed\fcpc\juridica.csv"

OUT_ENCODING = 'utf-8'



def make_pagamentos_pessoa_fisica():

    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data - pagamentos para pessoas físicas')

    filenames = os.listdir(PF_DIR)

    df = pd.DataFrame()

    for filename in filenames:
        
        df_ = pd.read_csv(os.path.join(PF_DIR, filename), **PF_READ_CFG)
        df_['filename'] = filename
        
        df = df.append(df_, ignore_index=True)
        
    df.rename(columns=PF_COLUMNS_RENAME, inplace=True)

    df.to_csv(PF_OUT_FILEPATH, index=False, encoding=OUT_ENCODING)


def make_pagamentos_pessoa_juridica():

    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data - pagamentos para pessoas jurídicas')

    filenames = os.listdir(PJ_DIR)

    df = pd.DataFrame()

    for filename in filenames:
        
        df_ = pd.read_csv(os.path.join(PJ_DIR, filename), **PJ_READ_CFG)
        df_['filename'] = filename
        
        df = df.append(df_, ignore_index=True)
        
    df.rename(columns=PJ_COLUMNS_RENAME, inplace=True)

    df.to_csv(PJ_OUT_FILEPATH, index=False, encoding=OUT_ENCODING)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    make_pagamentos_pessoa_fisica()
    make_pagamentos_pessoa_juridica()
