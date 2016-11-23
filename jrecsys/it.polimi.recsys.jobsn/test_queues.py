# -*- coding: utf-8 -*-
import pandas as pd
import queue as q
import os
from multiprocessing import Process
import scipy.sparse as sps

def similarity(i, j, interactions):
    
    count_i_j = len(pd.merge((pd.DataFrame(interactions[interactions['item_id'] == i]['user_id'])).drop_duplicates(), (pd.DataFrame(interactions[interactions['item_id'] == j]['user_id'])).drop_duplicates(), how='inner', on=['user_id']))
    count_j = len((pd.DataFrame(interactions[interactions['item_id'] == j]['user_id'])).drop_duplicates())
    
    return count_i_j/(count_j + 50)
     
class Worker(Process):
    def __init__(self, queue):
        super(Worker, self).__init__()
        self.queue= queue

    def run(self):
        print ('Worker started')
        # do some initialization here
        global similarity_matrix
        
        print ('Computing things!')
        for item in iter( self.queue.get, None ):
            users_interacted_this_item = pd.DataFrame({'user_id':interactions[interactions['item_id'] == item]['user_id']}).drop_duplicates()
            other_items_users_interacted_with  = pd.DataFrame({'item_id':interactions[(interactions['user_id'].isin(users_interacted_this_item['user_id'])) & ~(interactions['item_id'] == item)]['item_id']}).drop_duplicates()    
            
            local_similarity_matrix = pd.DataFrame({'item_id_a':item, 'item_id_b' : other_items_users_interacted_with['item_id'], 'coef' : 0})
            local_similarity_matrix['coef'] = local_similarity_matrix['item_id_b'].apply(lambda x: similarity(item, x, interactions))
            
            similarity_matrix = pd.concat([similarity_matrix, local_similarity_matrix], ignore_index=True)
            
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

request_queue = q.Queue()
for i in range(4):
    Worker( request_queue ).start()
for item in items:
    request_queue.put( item )
# Sentinel objects to allow clean shutdown: 1 per worker.
for i in range(4):
    request_queue.put( None ) 

