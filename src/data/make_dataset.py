# -*- coding: utf-8 -*-
import os
import click
import logging
import pandas as pd
import zipfile
import re

logger = logging.getLogger(__name__)

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

GD_COLUMNS_RENAME = {
	'Nome Órgao': 'orgao', 
	'Nome Elemento Despesa': 'elemento_despesa', 
	'Nome Função': 'funcao', 
	'Nome Subfunção': 'subfuncao',
	'Nome Programa': 'programa', 
	'Nome Ação': 'acao', 
	'Código Favorecido': 'cod_favorecido', 
	'Nome Favorecido': 'nome_favorecido', 
	'Data Pagamento': 'data', 
	'Valor': 'valor'
}

GD_READ_CFG = {
	"sep": "\t",
	"encoding": "iso-8859-1", 
	"decimal": ",",
	"usecols": GD_COLUMNS_RENAME.keys(),
	"iterator": True,
	"chunksize": 10000
}

GD_DIR = r"..\..\data\raw\federal\despesas\gastos diretos"
GD_OUT_FILEPATH = r"..\..\data\processed\federal\despesas\gastos diretos\ufc_{0}_{1}.csv"

OUT_ENCODING = 'utf-8'

NOME_ORGAO = 'UNIVERSIDADE FEDERAL DO CEARA'

EXTRACT_ANOMES = re.compile('\d{6}')

def read_zip_csv(filepath):

	logger.info('processing - {}'.format(filepath))
	
	with zipfile.ZipFile(filepath, 'r') as zip_ref:
		
		for filename in zip_ref.namelist():
			
			with zip_ref.open(filename) as zip_file:
		
				#http://stackoverflow.com/questions/13651117/pandas-filter-lines-on-load-in-read-csv
				iter_csv = pd.read_csv(zip_file, **GD_READ_CFG)

				df = pd.concat([chunk[chunk['Nome Órgao'] == NOME_ORGAO] for chunk in iter_csv])
				df.rename(columns=GD_COLUMNS_RENAME, inplace=True)

				df['data'] = pd.to_datetime(df.data, format="%d/%m/%Y", errors="coerce")
	
	return df

def make_gastos_diretos():

	logger.info('making final data set from raw data - gastos diretos')

	gastos_zips = [os.path.join(GD_DIR, filename) for filename in os.listdir(GD_DIR) if filename.endswith(".zip")]

	df = pd.DataFrame()

	dfs = []
	for zip_file in gastos_zips:
		
		dfs.append(read_zip_csv(zip_file))

	df = pd.concat(dfs, ignore_index=True)

	anos_meses = sorted([int(EXTRACT_ANOMES.findall(zip_file)[0]) for zip_file in gastos_zips])
	OUT_FILEPATH = GD_OUT_FILEPATH.format(anos_meses[0], anos_meses[-1])

	df.to_csv(OUT_FILEPATH, index=False)


def make_pagamentos_pessoa_fisica():

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
	make_gastos_diretos()
