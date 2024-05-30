def csv(path:str,col_key:str,col_value:str,sep:str):
    csv = open(path,'r').read().split('\n')
    header = csv[0]

    for col in [col_key,col_value]:
        if not col in header:
            raise KeyError(f'{col} not found in csv columns')
    
    index_col_key = header[:header.find(col_key)].count(sep)
    index_col_value = header[:header.find(col_value)].count(sep)

    return csv, index_col_key, index_col_value