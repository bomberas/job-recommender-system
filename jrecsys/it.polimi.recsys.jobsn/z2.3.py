# -*- coding: utf-8 -*-
"""
Created on Sun Oct 30 18:20:00 2016

@author: cecibloom
@version: 2.3
"""

import pandas as pd
from sklearn.cluster import KMeans
import datetime
import time
import os

print ('Executing z2.3.py at ', datetime.datetime.now())

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
    
def getRecommendations(user_id) :
    
    user_cluster_id = user_with_interactions[user_with_interactions['user_id'] == user_id]['cluster_id']
    print ('cluster', user_cluster_id)
    users_similar_to_me = pd.DataFrame({'user_id': user_with_interactions[user_with_interactions['cluster_id'].isin(user_cluster_id)]['user_id']}).drop_duplicates()
    print ('similars', users_similar_to_me)
    jobs_this_user_did_interact_with = pd.DataFrame({'item_id':interactions[interactions['user_id'] == user_id]['item_id']}).drop_duplicates()
    rated_items_by_users_similar_to_me = pd.DataFrame(interactions[(~interactions['item_id'].isin(jobs_this_user_did_interact_with['item_id']) & interactions['user_id'].isin(users_similar_to_me['user_id']))])
    best_rated_items_among_users_similar_to_me = pd.DataFrame(rated_items_by_users_similar_to_me.groupby('item_id').size().sort_values(ascending=False)[:5]).reset_index()
        
    result = '' + best_rated_items_among_users_similar_to_me['item_id'].astype(str).str.cat(sep=' ')
    
#    if not result:
#        return fillBlanks(user_id)
#    
    return result
            

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


print ('Starting process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 

    
user_with_interactions = pd.DataFrame(users[users['user_id'].isin(interactions['user_id'])]).drop_duplicates()
user_with_interactions_for_k_means = user_with_interactions[['career_level', 'discipline_id', 'industry_id', 'country', 'edu_degree']]
user_with_interactions = pd.DataFrame({'user_id' : user_with_interactions['user_id'], 'cluster_id' : 0})


dummies = pd.get_dummies(user_with_interactions_for_k_means['career_level']).rename(columns=lambda x: 'career_' + str(x))
df = pd.concat([user_with_interactions_for_k_means, dummies], axis=1)
user_with_interactions_for_k_means = df.drop(['career_level'], axis=1)

dummies = pd.get_dummies(user_with_interactions_for_k_means['discipline_id']).rename(columns=lambda x: 'discipline_' + str(x))
df = pd.concat([user_with_interactions_for_k_means, dummies], axis=1)
user_with_interactions_for_k_means = df.drop(['discipline_id'], axis=1)

dummies = pd.get_dummies(user_with_interactions_for_k_means['industry_id']).rename(columns=lambda x: 'industry_' + str(x))
df = pd.concat([user_with_interactions_for_k_means, dummies], axis=1)
user_with_interactions_for_k_means = df.drop(['industry_id'], axis=1)

dummies = pd.get_dummies(user_with_interactions_for_k_means['country']).rename(columns=lambda x: 'country_' + str(x))
df = pd.concat([user_with_interactions_for_k_means, dummies], axis=1)
user_with_interactions_for_k_means = df.drop(['country'], axis=1)

dummies = pd.get_dummies(user_with_interactions_for_k_means['edu_degree']).rename(columns=lambda x: 'degree_' + str(x))
df = pd.concat([user_with_interactions_for_k_means, dummies], axis=1)
user_with_interactions_for_k_means = df.drop(['edu_degree'], axis=1)

print ('Starting kmeans ... ', datetime.datetime.now())
kmeans = KMeans(n_clusters=24, random_state=10514141).fit(user_with_interactions_for_k_means)
labels = kmeans.labels_
user_with_interactions = pd.DataFrame({'user_id': user_with_interactions['user_id'], 'cluster_id': kmeans.labels_})
print ('Finishing kmeans ... ', datetime.datetime.now())

targets['recommended_items'] = targets['user_id'].apply(lambda x: getRecommendations(x))

print ('Finishing process at ', datetime.datetime.now(), ' [', time_in_millis(), ']') 

#print ('Saving data ... ', datetime.datetime.now())
#file = 'd:/recsys/deliverables/recsys_' + str(time_in_millis()) + '.csv'
#targets.astype(str).to_csv(file, sep=',', encoding='utf-8', index = False)
#print ('Done ... ', datetime.datetime.now())
