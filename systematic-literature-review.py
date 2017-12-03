#!/usr/bin/env python

# title: systematic-literature-review.py
# author: Paula
# version: 1
# status: development
# python_version: 3.5.2
# description: This python script read csv search exports from the 2017-2018 ACM and IEEE digital libraries to locate full-text articles, and the top 50 relevant searches based on the given keywords.

import pandas as pd
import re

def combineOriginal (id, kw):
	""" Combines all the search results for either ACM or IEEE digital library
	id : string as 'IEEE' or 'ACM' relating to the .csv naming convention
	kw : dataframe of keywords used. Keywords obtained from keywords.csv

	returns : data frame of search results 
	"""
	df = pd.DataFrame()
	for i in range(0,4):
		for j in range(0,4):
			# read and add keyword column
			temp = pd.read_csv('searches/%s0%d-%d.csv' % (id, i+1, j+1), error_bad_lines=False).assign(keyword=kw.iloc[i,j])
			###df = df.append(temp, ignore_index=True)
			df = pd.concat([df,temp]) 
	return(df)

def combineCSV(id, kw):
	""" Combines CSV results for ACM or IEEE by locating full-text articles, and the top 100 most relevant results.

	id : string as 'IEEE' or 'ACM' relating to the .csv naming convention
	kw : dataframe of keywords used. Keywords obtained from keywords.csv

	returns : data frame of search results. Also outputs removed data in .csv.
	"""
	df = pd.DataFrame()
	not_full_text = pd.DataFrame()
	not_top_relevant = pd.DataFrame()

	for i in range(0,4):
		for j in range(0,4):

			# read and add keyword column
			temp = pd.read_csv('searches/%s0%d-%d.csv' % (id, i+1, j+1), error_bad_lines=False).assign(keyword=kw.iloc[i,j])

			#handle object columns
			if id == 'IEEE':
				temp = temp[temp['Start Page'].astype(str).str.isdigit() & temp['End Page'].astype(str).str.isdigit()]
				temp['num_pages'] =  temp['End Page'].astype("int")-temp['Start Page'].astype("int")

			# keep full text publications
			not_full_text = pd.concat([not_full_text,temp[temp.num_pages < 5]])
			temp = temp[temp.num_pages >= 5]

			# reduce to top 50 relevant articles
			not_top_relevant = pd.concat([not_top_relevant, temp[50:]])
			temp = temp[:50]
			df = pd.concat([df,temp]) 


	not_full_text.to_csv('output/removed-data/%s-not-full-text.csv' % id)
	not_top_relevant.to_csv('output/removed-data/%s-not-top-relevant.csv' % id)
	return(df)

if __name__ == "__main__":
	keywords = pd.read_csv('searches/keywords.csv') 

	# combine results by publisher
	df_ACM = combineCSV('ACM', keywords)
	df_ACM["id"] = df_ACM["id"].fillna(0).astype(int)
	df_ACM = df_ACM.drop_duplicates(subset='id', keep="last")
	df_ACM.to_csv('output/ACM.csv')

	df_IEEE = combineCSV('IEEE', keywords)
	df_IEEE["PDF Link"] = df_IEEE["PDF Link"].fillna(0).astype(str)
	df_IEEE = df_IEEE.drop_duplicates(subset='PDF Link', keep="last")
	df_IEEE.to_csv('output/IEEE.csv')

