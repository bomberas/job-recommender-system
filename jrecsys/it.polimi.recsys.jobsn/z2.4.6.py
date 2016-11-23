# -*- coding: utf-8 -*-
"""
Created on Sat Nov 19 18:00:00 2016
cambiamos de similarity coefficient, esta vez sin popularity sumando y agrupando
@author: cecibloom
"""

import pandas as pd
import datetime
import time
import os

print ('Executing z2.4.6.py at ', datetime.datetime.now())

time_in_millis = lambda: int(round(time.time() * 1000))


def getRecommendations(target_user_id) :
    
    jobs_target_user_did_interact_with = pd.DataFrame(interactions[interactions['user_id'] == target_user_id])[['item_id','interaction_type']].sort_values(['item_id','interaction_type'], ascending=False)
    jobs_target_user_did_interact_with.drop_duplicates(['item_id'],keep='first', inplace=True)
    
    user_similars_to_me = pd.DataFrame(similarities[similarities['user_a_id'] == target_user_id])
    
    interactions_of_users = pd.merge(interactions, user_similars_to_me, on='user_id')[['user_id','item_id','jaccard_index','interaction_type']]
    jobs_similar_users = interactions_of_users[~interactions_of_users['item_id'].isin(jobs_target_user_did_interact_with['item_id'])]
    jobs_similar_users = jobs_similar_users.sort_values(['user_id','item_id','interaction_type'], ascending=False)
    jobs_similar_users.drop_duplicates(['item_id','user_id'],keep='first', inplace=True)

    frequencies = jobs_similar_users['item_id'].value_counts(normalize=True)
    popularity = pd.DataFrame({'item_id': frequencies.index.tolist(), 'popularity': frequencies}, index=None)
    jobs_similar_users = pd.merge(jobs_similar_users, popularity, on ='item_id')[['user_id','item_id','jaccard_index','interaction_type', 'popularity']]
    
    best_rated_items_among_users_similar_to_me = pd.DataFrame(jobs_similar_users.groupby('item_id').sum()).reset_index().sort_values('jaccard_index',ascending=False)[:5]

    return '' + best_rated_items_among_users_similar_to_me[:5]['item_id'].astype(str).str.cat(sep=' ')
            
    
print ('Loading data ... ', datetime.datetime.now())

interaction_cols = ['user_id', 'item_id', 'interaction_type', 'created_at']
interactions = pd.read_csv(os.environ['PATH_DS_INTERACTIONS'], sep='\t', names=interaction_cols,
                           header=0)

target_cols = ['user_id']
targets = pd.read_csv(os.environ['PATH_DS_TARGETS_TEST'], sep='\t', names=target_cols,
                           header=0)

similarity_cols = ['user_a_id', 'user_id', 'jaccard_index']
similarities = pd.read_csv(os.environ['PATH_DS_SIMILARITIES'], sep=',', names=similarity_cols)


"""
for every item i that u has no preference for yet

"""
print ('Starting process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 

targets['recommended_items'] = targets['user_id'].apply(lambda x: getRecommendations(x))

print ('Finishing process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 

print ('Saving data ... ', datetime.datetime.now())
file = 'd:/recsys/test/UCF_' + str(time_in_millis()) + '.csv'
targets.astype(str).to_csv(file, sep=',', encoding='utf-8', index = False)
print ('Done ... ', datetime.datetime.now())
