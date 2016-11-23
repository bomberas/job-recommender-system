# -*- coding: utf-8 -*-
"""
Created on Sun Oct 30 18:20:00 2016

Item based Collaborative Filtering

@author: cecibloom
@version: 2.3
"""

import pandas as pd
import datetime
import time
import os
import numpy as np
import scipy.sparse as sps
import logging
import argparse
from scipy.spatial.distance import cosine

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: %(name)s: %(levelname)s: %(message)s")

time_in_millis = lambda: int(round(time.time() * 1000))

def read_dataset():

    interaction_cols = ['user_id', 'item_id', 'interaction_type']
    interactions = pd.read_csv(os.environ['PATH_DS_INTERACTIONS'], sep='\t', names=interaction_cols, usecols=[0,1,2],
                               header=0)
    
    target_cols = ['user_id']
    targets = pd.read_csv(os.environ['PATH_DS_TARGETS_TEST'], sep='\t', names=target_cols,
                           header=0)
    
    interaction_items_cols = ['item_id']
    items_with_interactions = pd.read_csv(os.environ['PATH_DS_INTERACTION_ITEMS_TEST'], sep='\t', names=interaction_items_cols, usecols=[0],
                               header=0)
    
    return interactions, items_with_interactions, targets

def holdout_split(data, perc=0.8, seed=10111213):
    # set the random seed
    rng = np.random.RandomState(seed)
    # shuffle data
    nratings = data.shape[0]
    shuffle_idx = rng.permutation(nratings)
    train_size = int(nratings * perc)
    # split data according to the shuffled index and the holdout size
    train_split = data.ix[shuffle_idx[:train_size]]
    test_split = data.ix[shuffle_idx[train_size:]]
    return train_split, test_split
 
def similarity(i, j, interactions):
    
    count_i_j = len(pd.merge((pd.DataFrame(interactions[interactions['item_id'] == i]['user_id'])).drop_duplicates(), (pd.DataFrame(interactions[interactions['item_id'] == j]['user_id'])).drop_duplicates(), how='inner', on=['user_id']))
    count_j = len((pd.DataFrame(interactions[interactions['item_id'] == j]['user_id'])).drop_duplicates())
    
    return count_i_j/(count_j + 50)
     

def getSimilarItems(item, interactions):
    
    global similarity_matrix
    
    users_interacted_this_item = pd.DataFrame({'user_id':interactions[interactions['item_id'] == item]['user_id']}).drop_duplicates()
    other_items_users_interacted_with  = pd.DataFrame({'item_id':interactions[(interactions['user_id'].isin(users_interacted_this_item['user_id'])) & ~(interactions['item_id'] == item)]['item_id']}).drop_duplicates()    
    
    local_similarity_matrix = pd.DataFrame({'item_id_a':item, 'item_id_b' : other_items_users_interacted_with['item_id'], 'coef' : 0})
    c = time_in_millis()
    local_similarity_matrix['coef'] = local_similarity_matrix['item_id_b'].apply(lambda x: similarity(item, x, interactions))
    print ('At ', ' [', time_in_millis() - c, ']') 
    similarity_matrix = pd.concat([similarity_matrix, local_similarity_matrix], ignore_index=True)
    
    return other_items_users_interacted_with['item_id'].astype(str).str.cat(sep='|');


print ('Starting reading datasets at ', datetime.datetime.now(), ' [', time_in_millis(), ']')     
interactions, items, targets = read_dataset()
print ('Finishing reading datasets at ', datetime.datetime.now(), ' [', time_in_millis(), ']')     

similarity_matrix = pd.DataFrame(columns=['item_id_a','item_id_b','coef'])
similar_items = pd.DataFrame({'item_id':items['item_id'],'similars':'-'})

print ('Starting process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 
similar_items['similars'] = similar_items['item_id'].apply(lambda x: getSimilarItems(x, interactions))
print ('Finishing process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 

print ('Saving data ... ', datetime.datetime.now())
file = 'd:/recsys/test/' + str(time_in_millis()) + '.csv'
similarity_matrix.astype(str).to_csv(file, sep=',', encoding='utf-8', index = False)
print ('Done ... ', datetime.datetime.now())


