import os
import sys
import json
import shutil
from multiprocessing import Process

dir = os.path.dirname(os.path.dirname(__file__)) + '/'
sys.path.append(dir)

from src.utils import reader,mappers

def reduce_data(path:str,col_key:str,col_value:str,sep:str,chunk_size:int):

    if chunk_size < 100000:
        raise UserWarning('the default value to chunck size is higher than 100,000, lowers value can create higher use of CPU')

    csv, index_col_key, index_col_value = reader.csv(path,col_key,col_value,sep)

    number_mappers = int(len(csv)/chunk_size)
    number_mappers += 1 if len(csv)%chunk_size != 0 else 0

    os.mkdir('temp')
    list_mappers = []
    for i_mapper in range(number_mappers):
        if i_mapper == 0:
            list_mappers.append(Process(target=mappers.run,args=(csv[0:chunk_size],index_col_key,index_col_value,i_mapper)))
        elif i_mapper == number_mappers-1:
            list_mappers.append(Process(target=mappers.run,args=(csv[chunk_size*i_mapper:len(csv)],index_col_key,index_col_value,i_mapper)))
        else:
            list_mappers.append(Process(target=mappers.run,args=(csv[chunk_size*i_mapper:chunk_size*(i_mapper+1)],index_col_key,index_col_value,i_mapper)))

    for i,mapper in enumerate(list_mappers):
        print(f'Mapper {i}º started')
        mapper.start()

    for i,mapper in enumerate(list_mappers):
        mapper.join()
        print(f'Mapper {i}º ended')

    list_json_files = os.listdir('temp')
    aggreagate_date = {}
    for file in list_json_files:
        temp_data = json.loads(open(f'temp/{file}','r').read())

        keys = set(aggreagate_date.keys()).union(temp_data.keys())
        aggreagate_date = {key: aggreagate_date.get(key, 0) + temp_data.get(key, 0) for key in keys}

    shutil.rmtree('temp')

    return {key: aggreagate_date[key] for key in sorted(aggreagate_date)}