# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 16:32:12 2016
Pensamos que los mejor era buscar a los usuarios q se comportaron como yo
ose q interactuaron con los mismo items q yo
traer los otros items con los interactuaron q yo no
ordenarlos agruparlos
devolver 5
@author: cecibloom
"""

import pandas as pd
import datetime
import time

print ('Executing z.py at ', datetime.datetime.now())

time_in_millis = lambda: int(round(time.time() * 1000))


items_cols = ['item_a', 'item_b', 'similarity']
items_items_1 = pd.read_csv('d:/recsys/similarity1.csv', sep=',', names=items_cols,header=0)
items_items_2 = pd.read_csv('d:/recsys/similarity2.csv', sep=',', names=items_cols,header=0)
items_items_3 = pd.read_csv('d:/recsys/similarity3.csv', sep=',', names=items_cols,header=0)
items_items_4 = pd.read_csv('d:/recsys/similarity4.csv', sep=',', names=items_cols,header=0)
items_items_5 = pd.read_csv('d:/recsys/similarity5.csv', sep=',', names=items_cols,header=0)
items_items_6 = pd.read_csv('d:/recsys/similarity6.csv', sep=',', names=items_cols,header=0)
items_items_7 = pd.read_csv('d:/recsys/similarity7.csv', sep=',', names=items_cols,header=0)

similarity = pd.DataFrame(columns=items_cols)
similarity = pd.concat([similarity, items_items_1[items_items_1.similarity !=0.019607843137254902], 
                        items_items_2[items_items_2.similarity !=0.019607843137254902], 
                        items_items_3[items_items_3.similarity !=0.019607843137254902], 
                        items_items_4[items_items_4.similarity !=0.019607843137254902], 
                        items_items_5[items_items_5.similarity !=0.019607843137254902], 
                        items_items_6[items_items_6.similarity !=0.019607843137254902], 
                        items_items_7[items_items_7.similarity !=0.019607843137254902]], 
                        ignore_index=True)

print ('Saving data ... ', datetime.datetime.now())
file = 'd:/recsys/similarity_item_item__without_1_over_51.csv'
similarity.astype(str).to_csv(file, sep=',', encoding='utf-8', index = False)
print ('Done ... ', datetime.datetime.now())
# -*- coding: utf-8 -*-

