import sys
import json
import pandas as pd
from os.path import dirname,realpath

dir = dirname(dirname(__file__)) + '/'
sys.path.append(dir)

from src import MapReduce

def pandas():
    df = pd.read_csv(f'{dir}data/202101_BolsaFamilia_Pagamentos.csv',sep=';',decimal=',')

    df = df[['UF','VALOR PARCELA']]
    df = df.groupby(by='UF').sum().reset_index(drop=True)
    df.to_csv(f'{dir}data/aggregate_data.csv',sep=';',index=False)

def map_reduce():
    reduce_dict = MapReduce.reduce_data(path=f'{dir}data/202101_BolsaFamilia_Pagamentos.csv',
                      col_key='UF',
                      col_value='VALOR PARCELA',
                      sep=';',
                      chunk_size=1000000,
                      verbose=False)

    with open(f'{dir}data/reduce_data.json','w') as w_json:
        json.dump(reduce_dict,w_json)