# -*- coding: utf-8 -*-
"""
Created on Sun Oct 30 18:20:00 2016

@author: cecibloom
@version: 2.3
"""

import pandas as pd
import datetime
import time
import os

print ('Executing z2.3.py at ', datetime.datetime.now())

time_in_millis = lambda: int(round(time.time() * 1000))
            

print ('Loading data ... ', datetime.datetime.now())

interaction_cols = ['user_id', 'item_id', 'interaction_type', 'created_at']
interactions = pd.read_csv(os.environ['PATH_DS_INTERACTIONS'], sep='\t', names=interaction_cols,
                           header=0)

item_profile_cols = ['item_id', 'title', 'career_level', 'discipline_id', 'industry_id', 'country', 'region', 'latitude', 'longitude', 'employment', 'tags', 'created_at', 'active_during_test']
items = pd.read_csv(os.environ['PATH_DS_ITEMS'], sep='\t', names=item_profile_cols,
                           header=0)

user_profile_cols = ['user_id', 'jobroles', 'career_level', 'discipline_id', 'industry_id', 'country', 'region', 'experience_n_entries_class', 'experience_years_experience', 'experience_years_in_current', 'edu_degree', 'edu_fieldofstudies']
users = pd.read_csv(os.environ['PATH_DS_USERS'], sep='\t', names=user_profile_cols,
                           header=0)

target_cols = ['user_id']
targets = pd.read_csv(os.environ['PATH_DS_TARGETS_TEST'], sep='\t', names=target_cols,
                           header=0)

print ('Data fully loaded... ', datetime.datetime.now())

row_items_with_interactions = pd.DataFrame({'item_id' : items[items.item_id.isin(interactions['item_id'])]['item_id']})
row_items_with_interactions = row_items_with_interactions.sort(['item_id'],ascending=True)
col_items_with_interactions = row_items_with_interactions[:5]

print(col_items_with_interactions)
col_items_with_interactions = pd.DataFrame(['item_id'])
print(col_items_with_interactions)
similitud = pd.DataFrame(['item_id_A', 'item_id_B', 'coeff'])
#similitud['coeff'] = row_items_with_interactions['item_id'].apply(lambda x: )





#similitud_items = pd.DataFrame({'coeff':0})
#similitud_items['coeff'] = row_items_with_interactions['item_id'].apply(lambda x: getSimilars(x))
#




