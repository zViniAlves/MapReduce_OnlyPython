import sys
import time as t
from os.path import dirname,realpath

dir = dirname(dirname(__file__)) + '/'
sys.path.append(dir)

from tests.test_algorithms import pandas,map_reduce

start_pandas = t.time()
pandas()
end_pandas = t.time()
print('Pandas test ended')

start_map_reduce = t.time()
map_reduce()
end_map_reduce = t.time()
print('MapReduce test ended')

time_pandas = round((end_pandas - start_pandas),2)
time_map_reduce = round((end_map_reduce - start_map_reduce),2)

print('='*30)
print(f'The Map Reduce Algorithm taked {time_map_reduce} seconds to run')
print(f'The Pandas group by taked {time_pandas} seconds to run')

percentual_of_efficienty = round((((time_map_reduce-time_pandas)/time_pandas)*-1)*100,2)
print(f'Map Reduce is {percentual_of_efficienty}% better than pandas')