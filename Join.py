# -*- coding: utf-8 -*-
"""
Joing everything merge all of this later if needed
Created on Fri Feb  8 15:05:25 2019

@author: User
"""
import glob
import pandas as pd
import os

nd = pd.read_csv('data/nd_data.csv')
sd = pd.read_csv('data/sd_data.csv')
ne = pd.read_csv('data/ne_data.csv')
rvs = pd.read_csv('data/rvs_data.csv')

dfs = [nd, sd, ne, rvs]

for df in dfs:
    df = df[['year', 'county', 'state',
             'ag_district', 'yield', 'type', 'units']]
    print(df.columns)

final = pd.concat(dfs)
final = final[['year', 'county',
               'state', 'ag_district',
               'yield', 'type',
               'units']].sort_values(['county', 'year'], ascending=False)