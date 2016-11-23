# -*- coding: utf-8 -*-

from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool, TimeoutError
import pandas as pd
import os
import datetime
import time
import csv

time_in_millis = lambda: int(round(time.time() * 1000))

def similarity(i, j, interactions):
    
    count_i_j = len(pd.merge((pd.DataFrame(interactions[interactions['item_id'] == i]['user_id'])).drop_duplicates(), (pd.DataFrame(interactions[interactions['item_id'] == j]['user_id'])).drop_duplicates(), how='inner', on=['user_id']))
    count_j = len((pd.DataFrame(interactions[interactions['item_id'] == j]['user_id'])).drop_duplicates())
    
    return count_i_j/(count_j + 50)

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
    
interactions, items, targets = read_dataset()

def getSimilarItems(item):
    print('El item es: ', item)
   # global similarity_matrix
    users_interacted_this_item = pd.DataFrame({'user_id':interactions[interactions['item_id'] == item]['user_id']}).drop_duplicates()
    other_items_users_interacted_with  = pd.DataFrame({'item_id':interactions[(interactions['user_id'].isin(users_interacted_this_item['user_id'])) & ~(interactions['item_id'] == item)]['item_id']}).drop_duplicates()    
    
    local_similarity_matrix = pd.DataFrame({'item_id_a':item, 'item_id_b' : other_items_users_interacted_with['item_id'], 'coef' : 0})
    local_similarity_matrix['coef'] = local_similarity_matrix['item_id_b'].apply(lambda x: similarity(item, x, interactions))
    
    #similarity_matrix = pd.concat([similarity_matrix, local_similarity_matrix], ignore_index=True)    
    
    return local_similarity_matrix
    
    
# function to be mapped over
#def calculateParallel(items, threads=2):
#    pool = ThreadPool(threads)
#    results = pool.map(getSimilarItems, items['item_id'])
#    pool.close()
#    pool.join()
#    return results
#
#if __name__ == "__main__":
#    similarity_matrix = pd.DataFrame(columns=['item_id_a','item_id_b','coef'])
#    similar_items = pd.DataFrame({'item_id':items['item_id'],'similars':'-'})
#    
#    similarity_matrix = calculateParallel(items, 4)
#    
#    #print (similarity_matrix)    




if __name__ == '__main__':
    
    pool = Pool(processes=4)              # start 4 worker processes
    similarity_matrix = pd.DataFrame(columns=['item_id_a','item_id_b','coef'])
    
    
    print ('Starting process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 
    similarity_matrix = pool.apply_async(getSimilarItems, items['item_id'])
    print ('Finishing process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 
    
    
