import json
from os.path import dirname,realpath,exists
from src import MapReduce

dir = dirname(realpath(__file__)) + '/'

if not exists(f'{dir}data/202101_BolsaFamilia_Pagamentos.csv'):
    raise FileNotFoundError('Could not find the BolsFamilia csv, needs to be download from https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos')

reduce_dict = MapReduce.reduce_data(path=f'{dir}data/202101_BolsaFamilia_Pagamentos.csv',
                      col_key='UF',
                      col_value='VALOR PARCELA',
                      sep=';',
                      chunk_size=1000000,
                      verbose=True)

with open(f'{dir}data/reduce_data.json','w') as w_json:
    json.dump(reduce_dict,w_json)