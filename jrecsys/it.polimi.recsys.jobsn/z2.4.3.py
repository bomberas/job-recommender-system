# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 16:32:12 2016
Pensamos que los mejor era buscar a los usuarios q se comportaron como yo
ose q interactuaron con los mismo items q yo
traer los otros items con los interactuaron q yo no
ordenarlos agruparlos
devolver 5
2.4.1 con avg sin orden con problemas en los merge
2.4.2 sin avg con orden, aparentemente sin problemas en los merge
@author: tatibloom
"""

import pandas as pd
import datetime
import time
import os

print ('Executing z2.4.3.py at ', datetime.datetime.now())

time_in_millis = lambda: int(round(time.time() * 1000))

def content_based(user_id) :
    """
    devuelve todos los items cuya disciplina y pais sean igual a mi pais y disciplina
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

def item_based(user_id, recommendations, target_user_jobs) :
    print('item based')
    current_rec = recommendations['item_id'].astype(str).str.cat(sep=' ')
    len_rec = 5 - len(recommendations)
    
    recommendations.columns = ['user_id','item_b','jaccard_index','interaction_type', 'popularity']

    jobs_similar_to_user_jobs = pd.merge(similarities_item, recommendations, on= 'item_b')[['user_id','item_a','item_b','similarity']]
#    jobs_similar_to_user_jobs = pd.DataFrame(similarities_item[(similarities_item['item_b'].isin(recommendations['item_id']))]).sort_values(by='similarity',ascending=False)[:500]
    jobs_similar_to_user_jobs = pd.DataFrame(jobs_similar_to_user_jobs[~(jobs_similar_to_user_jobs['item_a'].isin((pd.concat(target_user_jobs,recommendations, ignore_index=True))['item_id']))])[:len_rec]
                                             
    return current_rec + ' ' + jobs_similar_to_user_jobs['item_a'].astype(int).astype(str).str.cat(sep=' ')
    
def getRecommendations(target_user_id) :
    
    jobs_target_user_did_interact_with = pd.DataFrame({'item_id':interactions[interactions['user_id'] == target_user_id]['item_id']}).drop_duplicates()
    
    user_similars_to_me = pd.DataFrame(similarities[similarities['user_a_id'] == target_user_id])
    
    interactions_of_users = pd.merge(interactions, user_similars_to_me, on='user_id')[['user_id','item_id','jaccard_index','interaction_type']]
    jobs_similar_users = interactions_of_users[~interactions_of_users['item_id'].isin(jobs_target_user_did_interact_with['item_id'])]
    jobs_similar_users = jobs_similar_users.sort_values(['user_id','item_id','interaction_type'], ascending=False)
    jobs_similar_users.drop_duplicates(['item_id','user_id'],keep='first', inplace=True)

    frequencies = jobs_similar_users['item_id'].value_counts(normalize=True)
    popularity = pd.DataFrame({'item_id': frequencies.index.tolist(), 'popularity': frequencies}, index=None)
    jobs_similar_users = pd.merge(jobs_similar_users, popularity, on ='item_id')[['user_id','item_id','jaccard_index','interaction_type', 'popularity']]
    
    jobs_similar_users['rating'] =  jobs_similar_users['jaccard_index'] * jobs_similar_users['popularity']
    
    jobs_similar_users = jobs_similar_users.sort_values('rating',ascending=False)
    jobs_similar_users.drop_duplicates(['item_id'],keep='first', inplace=True)
    
    best_rated_items_among_users_similar_to_me = jobs_similar_users[:5]
    
    if best_rated_items_among_users_similar_to_me.empty:
        return content_based(target_user_id)
    
    if len(best_rated_items_among_users_similar_to_me) < 5:
        return item_based(target_user_id, best_rated_items_among_users_similar_to_me, jobs_target_user_did_interact_with)         
    else:
        return '' + best_rated_items_among_users_similar_to_me['item_id'].astype(str).str.cat(sep=' ')
            
    
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

similarity_cols = ['user_a_id', 'user_id', 'jaccard_index']
similarities = pd.read_csv(os.environ['PATH_DS_SIMILARITIES'], sep=';', names=similarity_cols)

similarity_cols = ['item_a', 'item_b', 'similarity']
similarities_item = pd.read_csv('d:/recsys/similarity_item_item.csv', sep=',', names=similarity_cols)




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
