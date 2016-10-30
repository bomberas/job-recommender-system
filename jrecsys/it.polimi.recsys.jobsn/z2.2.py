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

def fillBlanks(user_id) :
    """
    devuelve todos lso items cuya disciplina y pais sean igual a mi pais y disciplina
    busca estos items en interacciones y grupealos
    
    """
    mycolumns = ['item_id']
    result = pd.DataFrame(columns=mycolumns)
	
    user_discipline = users[users.user_id == user_id]['discipline_id']
    user_country = users[users.user_id == user_id]['country']
    user_industry = users[users.user_id == user_id]['industry_id']
	
    jobs_this_user_did_interact_with = pd.DataFrame({'item_id':interactions[interactions['user_id'] == user_id]['item_id']}).drop_duplicates()
	
    items_similar_to_me_in_discipline_and_country = pd.DataFrame({'item_id':items[(items['discipline_id'].isin(user_discipline) & items['country'].isin(user_country) & ~items['item_id'].isin(jobs_this_user_did_interact_with))]['item_id']}).drop_duplicates()
    best_rated_items_similar_to_me_in_discipline_and_country = pd.DataFrame(interactions[interactions['item_id'].isin(items_similar_to_me_in_discipline_and_country['item_id'])]).groupby('item_id').size().sort_values(ascending=False)[:5].reset_index()
    
    result = pd.concat([result, best_rated_items_similar_to_me_in_discipline_and_country], ignore_index=True)
    result.drop_duplicates()	

    
    if (len(result) < 5) :
		
        items_similar_to_me_in_industry = pd.DataFrame({'item_id':items[(items['industry_id'].isin(user_industry) & items['country'].isin(user_country) & ~items['item_id'].isin(jobs_this_user_did_interact_with))]['item_id']}).drop_duplicates()
        best_rated_items_similar_to_me_in_industry = pd.DataFrame(interactions[(interactions['item_id'].isin(items_similar_to_me_in_industry['item_id']) & ~interactions['item_id'].isin(result))]).groupby('item_id').size().sort_values(ascending=False)[:5].reset_index()
		
        result = pd.concat([result, best_rated_items_similar_to_me_in_industry], ignore_index=True)
        
        if (len(result) < 5) :
		
            items_similar_to_me_in_country = pd.DataFrame({'item_id':items[~items['item_id'].isin(jobs_this_user_did_interact_with) & items['country'].isin(user_country)]['item_id']}).drop_duplicates()
            best_rated_items_similar_to_me_in_country = pd.DataFrame(interactions[(interactions['item_id'].isin(items_similar_to_me_in_country['item_id']) & ~interactions['item_id'].isin(result))]).groupby('item_id').size().sort_values(ascending=False)[:5].reset_index()
            
            result = pd.concat([result, best_rated_items_similar_to_me_in_country], ignore_index=True)
    
    result = result.astype(int)[:5]
    return result['item_id'].astype(str).str.cat(sep=' ');
    
def getSimilars(user_id) :
    
    jobs_this_user_did_interact_with = pd.DataFrame({'item_id':interactions[interactions['user_id'] == user_id]['item_id']}).drop_duplicates()
    users_did_interact_with_these_jobs = pd.DataFrame({'user_id' : interactions[(interactions.user_id != user_id) & (interactions['item_id'].isin(jobs_this_user_did_interact_with['item_id']))]['user_id']}).drop_duplicates()
    rated_items_by_users_similar_to_me = pd.DataFrame(interactions[(~interactions['item_id'].isin(jobs_this_user_did_interact_with['item_id']) & interactions['user_id'].isin(users_did_interact_with_these_jobs['user_id']))])
    best_rated_items_among_users_similar_to_me = pd.DataFrame(rated_items_by_users_similar_to_me.groupby('item_id').size().sort_values(ascending=False)[:5]).reset_index()
        
    result = '' + best_rated_items_among_users_similar_to_me['item_id'].astype(str).str.cat(sep=' ')
    
    if not result:
        return fillBlanks(user_id)
    
    return result
            

print ('Loading data ... ', datetime.datetime.now())

interaction_cols = ['user_id', 'item_id', 'interaction_type', 'created_at']
interactions = pd.read_csv('d:/recsys/interactions.csv', sep='\t', names=interaction_cols,
                           header=0)

item_profile_cols = ['item_id', 'title', 'career_level', 'discipline_id', 'industry_id', 'country', 'region', 'latitude', 'longitude', 'employment', 'tags', 'created_at', 'active_during_test']
items = pd.read_csv('d:/recsys/item_profile.csv', sep='\t', names=item_profile_cols,
                           header=0)

user_profile_cols = ['user_id', 'jobroles', 'career_level', 'discipline_id', 'industry_id', 'country', 'region', 'experience_n_entries_class', 'experience_years_experience', 'experience_years_in_current', 'edu_degree', 'edu_fieldofstudies']
users = pd.read_csv('d:/recsys/user_profile.csv', sep='\t', names=user_profile_cols,
                           header=0)

target_cols = ['user_id']
targets = pd.read_csv('d:/recsys/target_users_test.csv', sep='\t', names=target_cols,
                           header=0)


"""
for every item i that u has no preference for yet

"""
print ('Starting process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 

targets['recommended_items'] = targets['user_id'].apply(lambda x: getSimilars(x))

print ('Finishing process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 

print ('Saving data ... ', datetime.datetime.now())
file = 'd:/recsys/test_' + str(time_in_millis()) + '.csv'
targets.astype(str).to_csv(file, sep=',', encoding='utf-8', index = False)
print ('Done ... ', datetime.datetime.now())
