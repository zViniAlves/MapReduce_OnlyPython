import json
import numpy as np

def run(csv:list,index_col_key:int,index_col_value:int,i_mapper):
    dict_map = {}
    for row in csv[1:-1]:
        row = row.split(";")

        key = row[index_col_key]
        value = row[index_col_value]

        if key[0] == '"' and key[-1] == '"':
            key = key[1:-1]

        if value[0] == '"' and value[-1] == '"':
            value = value[1:-1]

        value = value.replace(',','.') if ',' in value else value

        value = float(value)
        if key in dict_map.keys():
            dict_map[key].append(value)
        else:
            dict_map[key] = [value]

    for key in dict_map:
        dict_map[key] = sum(dict_map[key])

    with open(f'temp/data_{i_mapper}.json','w') as out_file:
        json.dump(dict_map,out_file)