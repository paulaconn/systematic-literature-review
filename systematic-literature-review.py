#!/usr/bin/env python

# title: systematic-literature-review.py
# author: Paula
# version: 2
# status: development
# python_version: 3.5.2
# description: This python script reads bulk csv search results from the 2017-2018 ACM and IEEE digital libraries to locate full-text articles, and the top 50 relevant searches based on the given keywords.  

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
            df = pd.concat([df,temp]) 

    df.to_csv('output/%s-original.csv' % id)

    return(df)

def formatSearches(id, kw):
    """ Formats .CSV search results to have consistent 'author' and 'title' headings in IEEE and ACM. Also removes non-full text articles (<5 pages).

    id : string as 'IEEE' or 'ACM' relating to the .csv naming convention
    kw : dataframe of keywords used. Keywords obtained from keywords.csv

    returns : null. Writes the formatted csv files in the searches directory and outputs documentation of removed results.
    """
    df_rm_short = pd.DataFrame()

    for i in range(0,4):
        for j in range(0,4):

            # read and add keyword column
            temp = pd.read_csv('searches/%s0%d-%d.csv' % (id, i+1, j+1), error_bad_lines=False).assign(keyword=kw.iloc[i,j])

            # handle object columns
            if id == 'IEEE':
                temp = temp[temp['Start Page'].astype(str).str.isdigit() & temp['End Page'].astype(str).str.isdigit()]
                temp['num_pages'] =  temp['End Page'].astype("int")-temp['Start Page'].astype("int")
                temp.rename(columns={'Document Title': 'title', 'Authors': 'author'}, inplace=True)

            # keep full text publications
            df_rm_short = pd.concat([df_rm_short,temp[temp.num_pages < 5]])
            temp = temp[temp.num_pages >= 5]

            temp.to_csv('searches/%s0%d-%d.csv' % (id, i+1, j+1), error_bad_lines=False)

    df_rm_short.to_csv('output/removed-data/%s-rm-short.csv' % id)

def combineCSV(id, kw):
    """ Reads formatted CSV files and combines them into a master CSV with no duplicate results, and only the top 50 relevant searches.

    id : string as 'IEEE' or 'ACM' relating to the .csv naming convention
    kw : dataframe of keywords used. Keywords obtained from keywords.csv

    returns : null. Writes the formatted csv files and documentation of any removed rows in the output directory.
    """
    df_main = pd.DataFrame()
    df_rm_relevant = pd.DataFrame()

    idx = 0
    for i in range(0,4):
        for j in range(0,4):
            # read each CSV
            temp = pd.read_csv('searches/%s0%d-%d.csv' % (id, i+1, j+1), error_bad_lines=False)

            # combine master and temp dfs
            df_main = pd.concat([df_main, temp])

            #document duplicate search results
            df_rm_duplicate = df_main[df_main.duplicated(subset=['title', 'author'], keep=False)]

            # remove duplicates
            df_main = df_main.drop_duplicates(subset=['title', 'author']) 

            # keep top 50 relevant
            idx += 50

            # document removed files
            df_rm_relevant = pd.concat([df_rm_relevant, df_main[idx:]])

            # remove duplicates now
            df_main = df_main[:idx]
    
    df_main.to_csv('output/%s.csv' % (id), error_bad_lines=False)
    df_rm_duplicate.to_csv('output/removed-data/%s-rm-duplicate.csv' % id)
    df_rm_relevant.to_csv('output/removed-data/%s-rm-relevant.csv' % id)

    return(df_main)


if __name__ == "__main__":
    keywords = pd.read_csv('searches/keywords.csv') 

    # if searches need to be formatted uncomment lines below. combineOriginal method is useful for troubleshooting only. Otherwise not necessary to run:

    #combineOriginal('ACM', keywords)
    #formatSearches('ACM', keywords)
    df_ACM = combineCSV('ACM', keywords)

    #combineOriginal('IEEE', keywords)
    #formatSearches('IEEE', keywords)
    df_ACM = combineCSV('IEEE', keywords)

