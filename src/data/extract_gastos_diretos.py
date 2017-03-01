# -*- coding: utf-8 -*-
import os
import click
import logging
import pandas as pd
import zipfile
import re

def STRIP(text):

	try:
		return text.strip()
	except AttributeError:
		return text

class GastosDiretosExtractor:

	COLUMNS_RENAME = {
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

	READ_CFG = {
		"sep": "\t",
		"encoding": "iso-8859-1", 
		"decimal": ",",
		"usecols": COLUMNS_RENAME.keys(),
		"iterator": True,
		"chunksize": 10000,
		"converters": {column_name:STRIP for column_name, _ in COLUMNS_RENAME.items()}
	}

	EXTRACT_ANOMES = re.compile('\d{6}')

	OUT_FILENAME_TEMPLATE = "{0}_{1}_{2}.csv"

	def __init__(self, nome, raw_dir, out_dir, filtros):

		self.nome = nome
		self.filtros = filtros
		self.raw_dir = raw_dir
		self.out_dir = out_dir
		self.logger = logging.getLogger("GastosDiretosExtractor")

	def filtrar(self, df):

		return df[df[list(self.filtros.keys())].isin(self.filtros.values()).all(axis=1)]

	def ler_zip_csv(self, filepath):

		self.logger.info('processando - {}'.format(filepath))
		
		with zipfile.ZipFile(filepath, 'r') as zip_ref:
			
			for filename in zip_ref.namelist():
				
				with zip_ref.open(filename) as zip_file:
			
					#http://stackoverflow.com/questions/13651117/pandas-filter-lines-on-load-in-read-csv
					iter_csv = pd.read_csv(zip_file, **GastosDiretosExtractor.READ_CFG)

					df = pd.concat([self.filtrar(chunk) for chunk in iter_csv])
					df.rename(columns=GastosDiretosExtractor.COLUMNS_RENAME, inplace=True)

					df['data'] = pd.to_datetime(df.data, format="%d/%m/%Y", errors="coerce")
		
		return df

	def extrair_ano_mes(self, filepath):

		return int(GastosDiretosExtractor.EXTRACT_ANOMES.findall(filepath)[0])

	def processar(self):

		self.logger.info('processando a partir dos dados brutos')

		gastos_zips = [os.path.join(self.raw_dir, filename) for filename in os.listdir(self.raw_dir) if filename.endswith(".zip")]

		df = pd.DataFrame()
		dfs = []

		for zip_file in gastos_zips:
			
			dfs.append(self.ler_zip_csv(zip_file))

		df = pd.concat(dfs, ignore_index=True)

		anos_meses = sorted([self.extrair_ano_mes(zip_file) for zip_file in gastos_zips])
		menor_ano_mes = anos_meses[0]
		maior_ano_mes = anos_meses[-1]

		out_filename = GastosDiretosExtractor.OUT_FILENAME_TEMPLATE.format(self.nome, menor_ano_mes, maior_ano_mes)

		out_filepath = os.path.join(self.out_dir, out_filename)

		df.to_csv(out_filepath, index=False, encoding='utf-8')


if __name__ == "__main__":

	log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
	logging.basicConfig(level=logging.INFO, format=log_fmt)

	raw_dir = r"..\..\data\raw\federal\despesas\gastos diretos"
	out_dir = r"..\..\data\processed\federal\despesas\gastos diretos"
	filtros = {'Nome Órgao': 'UNIVERSIDADE FEDERAL DO PARANA'}

	gde = GastosDiretosExtractor('ufpr', raw_dir, out_dir, filtros)

	gde.processar()
	
	filtros = {'Nome Órgao': 'UNIVERSIDADE FEDERAL DO CEARA'}

	gde = GastosDiretosExtractor('ufc', raw_dir, out_dir, filtros)

	gde.processar()