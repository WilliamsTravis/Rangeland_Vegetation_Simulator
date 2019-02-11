# -*- coding: utf-8 -*-
"""
This one will identify the counties and measurements with the most consistent
histories. My first guess is that there is much less data on non-irrigated
wheat. Then it will find the counties neighboring the most consistent county
in each state, and find all of the rvs data. 

Created on Fri Feb  8 11:33:24 2019

@author: User
"""
import pandas as pd
from functions import countyTops, countyRVS

# The three carefully queried datasets from NASS
df_paths = ['data/wheat_nass_sd.csv', 'data/wheat_nass_nd.csv',
            'data/wheat_nass_ne.csv']

# This saves each states data to file and return a list of top and neighboring
# counties
counties = []
for p in df_paths:
    counties.append(countyTops(p))

# This flattens the county list
counties = [c for sl in counties for c in sl]

# This finds the Rangeland Vegetation Simulator data for each county
countyRVS(counties)

# This joins all the files together
nd = pd.read_csv('data/nd_data.csv')
sd = pd.read_csv('data/sd_data.csv')
ne = pd.read_csv('data/ne_data.csv')
rvs = pd.read_csv('data/rvs_data.csv')
dfs = [nd, sd, ne]
top_counties = []

# Add the star
for df in dfs:
    top_county = df.county[df['county'].str.contains('\*')].unique()[0]
    top_state = df.state[df['county'].str.contains('\*')].unique()[0]
    top_counties.append([top_county.replace('*', ''), top_state])

def applyStar(string, c):
    if string == c[0]:
        string = string + '*'
    return string

for c in top_counties:
    rvs.county = rvs.county.apply(lambda x: applyStar(x, c))

# Fill in the ag districts
district_df = pd.concat(dfs)
district_df = district_df[['county', 'state', 'ag_district']]
district_df = district_df.drop_duplicates()
district_df['place'] = district_df['county'] + '_' + district_df['state']
district_df = district_df[['place', 'ag_district']]
rvs['place'] = rvs['county'] + '_' + rvs['state']
rvs = rvs.merge(district_df, on='place')
rvs = rvs.drop(['place'], axis=1)
rvs = rvs[['year', 'county', 'state',
           'ag_district', 'yield', 'type', 'units']]

dfs.append(rvs)
for df in dfs:
    df = df[['year', 'county', 'state',
             'ag_district', 'yield', 'type', 'units']]
    print(df.columns)

final = pd.concat(dfs)
final = final[['year', 'county',
               'state', 'ag_district',
               'yield', 'type',
               'units']].sort_values(['county', 'year'], ascending=False)

final.to_csv('data/wheat_rvs_data_02092019.csv', index=False, na_rep='nan')
