# -*- coding: utf-8 -*-
"""
Created on Sun Oct 30 18:20:00 2016

Changed from conditional probablities as similarity to Tanimoto coefficient
@author: tatibloom
@version: 2.3
"""

import numpy as np
import scipy.sparse as sps
import pandas as pd
import logging
import argparse
import os
import time
import csv
import multiprocessing

#from metrics import roc_auc, precision, recall, map, ndcg, rr
#from datetime import datetime as dt

time_in_millis = lambda: int(round(time.time() * 1000))

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: %(name)s: %(levelname)s: %(message)s")


def read_dataset(path, header=None, columns=None, user_key='user_id', item_key='item_id', rating_key='interaction_type', sep=','):
	data = pd.read_csv(path, header=header, names=columns, sep=sep)
	logger.info('Columns: {}'.format(data.columns.values))
	# build user and item maps (and reverse maps)
	# this is used to map ids to indexes starting from 0 to nitems (or nusers)
	items = data[item_key].unique()
	users = data[user_key].unique()
	item_to_idx = pd.Series(data=np.arange(len(items)), index=items)
	user_to_idx = pd.Series(data=np.arange(len(users)), index=users)
	idx_to_item = pd.Series(index=item_to_idx.data, data=item_to_idx.index)
	idx_to_user = pd.Series(index=user_to_idx.data, data=user_to_idx.index)
	# map ids to indices
	data['item_idx'] = item_to_idx[data[item_key].values].values
	data['user_idx'] = user_to_idx[data[user_key].values].values
	return data, idx_to_user, idx_to_item

def holdout_split(data, perc=0.8, seed=1234):
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

def df_to_csr(df, nrows, ncols, user_key='user_idx', item_key='item_idx', rating_key='interaction_type'):
	rows = df[user_key].values
	columns = df[item_key].values
	ratings = df[rating_key].values
	shape = (nrows, ncols)
	# using the 4th constructor of csr_matrix 
	# reference: https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html
	return sps.csr_matrix((ratings, (rows, columns)), shape=shape)

class TopPop(object):
	"""Top Popular recommender"""
	def __init__(self):
		super(TopPop, self).__init__()
	def fit(self, train):
		if not isinstance(train, sps.csc_matrix):
			# convert to csc matrix for faster column-wise sum
			train_csc = train.tocsc()
		else:
			train_csc = train
		item_pop = (train_csc > 0).sum(axis=0)	# this command returns a numpy.matrix of size (1, nitems)
		item_pop = np.asarray(item_pop).squeeze() # necessary to convert it into a numpy.array of size (nitems,)
		self.pop = np.argsort(item_pop)[::-1]
	def recommend(self, profile, k=None, exclude_seen=True):
		unseen_mask = np.in1d(self.pop, profile, assume_unique=False, invert=True)
		return self.pop[unseen_mask][:k]
		
def getSimilarity(item_a,item_b,train,similes):
    n_axb = train[:,item_a].multiply(train[:,item_b]).sum()
    n_a = train[:,item_a].sum()
    n_b = train[:,item_b].sum()
    coeficient_of_similarity = 0
    
    if n_b > 10:
        coeficient_of_similarity = n_axb / (n_a + n_b - n_axb)
        similes.append([idx_to_item[item_a],
				idx_to_item[item_b],
				coeficient_of_similarity])

def my_execute(item_a):
	global train
	similes = []
	items_id_by_users_that_rank_item_a = train[:,item_a].multiply(train).nonzero()[1].tolist()
	df = pd.DataFrame(data=list(items_id_by_users_that_rank_item_a),index=range(len(list(items_id_by_users_that_rank_item_a))),columns=[0]).drop_duplicates()
	df[0].apply(lambda item_b: getSimilarity(item_a,item_b,train,similes))
	return similes

# let's use an ArgumentParser to read input arguments
parser = argparse.ArgumentParser()
parser.add_argument('--dataset', type=str, default=os.environ['PATH_DS_INTERACTIONS_ORDERED'])
parser.add_argument('--holdout_perc', type=float, default=1.0)
parser.add_argument('--header', type=int, default=0)
parser.add_argument('--columns', type=str, default=None)
parser.add_argument('--sep', type=str, default='\t')
parser.add_argument('--user_key', type=str, default='user_id')
parser.add_argument('--item_key', type=str, default='item_id')
parser.add_argument('--rating_key', type=str, default='interaction_type')
parser.add_argument('--rnd_seed', type=int, default=1234)
args = parser.parse_args()
		
# convert the column argument to list
if args.columns is not None:
	args.columns = args.columns.split(',')
	
# read the dataset
logger.info('Reading {}'.format(args.dataset))
dataset, idx_to_user, idx_to_item = read_dataset(
	args.dataset, 
	header=args.header,
	sep=args.sep,
	columns=args.columns,
	item_key=args.item_key,
	user_key=args.user_key,
	rating_key=args.rating_key)
		
nusers, nitems = len(idx_to_user), len(idx_to_item)
logger.info('The dataset has {} users and {} items'.format(nusers, nitems))
		
# compute the holdout split
logger.info('Computing the {:.0f}% holdout split'.format(args.holdout_perc*100))
train_df, test_df = holdout_split(dataset, perc=args.holdout_perc, seed=args.rnd_seed)
train = df_to_csr(train_df, nrows=nusers, ncols=nitems)
#test = df_to_csr(test_df, nrows=nusers, ncols=nitems)
		
similes = []
logger.info("Starting process")

#extractor = parallelTestModule.ParallelExtractor()
#extractor.runInParallel(numProcesses=2, numThreads=4)
if __name__ == '__main__':
	p = multiprocessing.Pool(4)
	#p = multiprocessing.Process(target=my_execute, args=[0])
	#p.start()
	similes = p.map(my_execute,range(1100,11100))
	#assume we have a function:
	#exection.run_main_with_args(filename,name,today_str,dbfolder)
	
	"""for item_a in range(nitems):
		users_per_item_a = train[:,item_a]
		#items_id_per_user = users_per_item_a.nonzero()[0].tolist()
		#items_per_user = set()
		#for user_item in items_id_per_user:
		#	items_per_user.update(train[user_item].nonzero()[1].tolist())
		items_id_by_users_that_rank_item_a = train[:,item_a].multiply(train).nonzero()[1].tolist()
		df = pd.DataFrame(data=list(items_id_by_users_that_rank_item_a),index=range(len(list(items_id_by_users_that_rank_item_a))),columns=[0]).drop_duplicates()
		df[0].apply(lambda item_b: getSimilarity(item_a,item_b,train,similes))
	"""
	logger.info("Finishing process")
		#logger.info(similes)
	with open('d:/similarity_matrix_tanimoto_' + str(time_in_millis()) +'.csv', 'w', newline='') as fp:
		a = csv.writer(fp, delimiter=',')
		a.writerows(similes)


