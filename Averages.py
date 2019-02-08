# -*- coding: utf-8 -*-
"""
Working with the rangeland vegetation simulator. 

This is new territory for me, I haven't had to work with raster data too
big for memory.


Created on Thu Feb  7 09:46:52 2019

@author: User
"""
import glob
import geopandas as gpd
import os
from osgeo import gdal
import warnings
os.chdir(r"c:\users\user\github\rangeland_vegetation_simulator")
from functions import *
warnings.filterwarnings('ignore')

# Set up data paths
shape_path = 'D:\\data\\shapefiles\\nad83'
prefab_path = "D:\\data\\RPMS_RangeProd_For_Posting\\tifs\\nad83"
gdalwarp = 'C:/ProgramData/Anaconda3/Library/bin/gdalwarp.exe'

# Clip counties - the whole thing is too big
all_counties = gpd.read_file(os.path.join(shape_path,
                                          'cb_2017_us_county_500k.shp'))
state_info = pd.read_csv(os.path.join(shape_path, 'us-state-ansi-fips.csv'))

# One county at a time. 'county_stateabbr'
counties = ['harding_sd', 'butte_sd', 'sioux_ne', 'scotts_bluff_ne',
            'mckenzie_nd', 'williams_nd']

# Create individual county shapes
def getCounty(all_counties, county_string):
    all_counties['NAME'] = all_counties['NAME'].apply(lambda x: x.upper())
    state_abbr = county_string.split('_')[-1].upper()
    county_name = " ".join(county_string.split('_')[:-1]).upper()
    state_fips = state_info.st[state_info.stusps == state_abbr]
    state_fips = "{:02d}".format(int(state_fips))
    county = all_counties[(all_counties['NAME'] == county_name) &
                          (all_counties['STATEFP'] == state_fips)]
    # Write this to a temporary file in the repository
    county.to_file('data/temp_county.shp')
    return [county_name, state_abbr]

# This helps with naming
years = {i: 1983 + i for i in range(1, 36)}

# Create a clipped county rvs rasters for each county (then delete?)
rvs_dfs = []
for county_string in tqdm(counties, position=0):
    [county_name, state_abbr] = getCounty(all_counties, county_string)
    means = {}

    # clip each huge rvs tiff to a small rvs tiff
    for f in glob.glob(os.path.join(prefab_path, "*tif")):
        name = os.path.basename(f).split('.')[0]
        index = int("".join([s for s in name[-2:] if s.isnumeric()]))
        year = years[index]     
        output = 'data/rvs_clips/clip.tif'.format(year)
        gdal.Warp(output, f,
                  cutlineDSName='data/temp_county.shp',
                  cropToCutline=True)
        clip_info = gdal.Info('data/rvs_clips/clip.tif', options=['-stats'])
        clip_info = clip_info.split('\n')
        clip_info = [c for c in clip_info if "STATISTICS_MEAN" in c][0]
        mean = float(clip_info[clip_info.index('=') + 1:])
        means[year] = mean
    
    # Create df
    df = pd.DataFrame([means]).T
    df.columns = ['rangeland_lb_acre']
    df['county'] = county_name
    df['state'] = state_abbr
    rvs_dfs.append(df)

# Create one dataframe
rvs_df = pd.concat(rvs_dfs)

# Add wheat production data
wheat = pd.read_csv('data/nass_wheat_yields.csv')
wheat = wheat[['Year', 'County', 'State', 'Data Item', 'Value']]

# remove Sioux ND
wheat['County_State'] = wheat['County'] + '_' + wheat['State'] 
wheat = wheat[wheat['County_State'] != 'SIOUX_NORTH DAKOTA']

# They organize this oddly
items = wheat['Data Item'].unique()

# We're going to change these data item names
def keepThese(string):
    keepers = ['WHEAT', 'WINTER', 'SPRING', 'BU', 'ACRE', 'ACRES', 'HARVESTED',
               'PLANTED']
    string = string.replace(",", "")
    lst = [s for s in string.split() if s in keepers]    
    new_name = "_".join(lst)
    return new_name

# Create a dictionary of new names and old names
item_dict = {d: keepThese(d) for d in items}

# Now go ahead and change the original df
wheat['Data Item'] = wheat['Data Item'].map(item_dict)
items = wheat['Data Item'].unique()

# We want each data item t have it's own column
dfs = [wheat[wheat['Data Item'] == di] for di in items]

# Removing offending columns and other nonsense
def fixNonsense(df):
    df.Value = df.Value.apply(lambda x: float(x.replace(",", "")))
    item = df['Data Item'].unique()[0]
    new_name = item.lower()
    df = df.drop(['Data Item'], axis=1)
    df.rename(columns={'Value': new_name}, inplace=True)
    df.columns = [c.lower() for c in df.columns]

    # New strategy
    df.index = df[['year', 'county', 'state']]
    df = df.drop(['year', 'county', 'state', 'county_state'], axis=1)

    # Old strategy
    # df['join'] = df.year.astype(str) + df.county + df.state
    return df

# Now we are rushed - Fix this later
fixed = [fixNonsense(df) for df in dfs]
items = [i.lower() for i in items]  # Now we can change this

# Now to merge all of these together
# Start with one
wheat_df = fixed[0]

# Use only the production and join fields from the others
for i in range(1, len(fixed)):
    next_column = fixed[i]
    wheat_df = wheat_df.join(next_column, how='outer')

# Join rangeland data
rvs = rvs_df.copy()    
rvs['year'] = rvs.index
state_info.columns = ['state_name', 'st', 'state']
state_info['state_name'] = state_info['state_name'].apply(lambda x: x.upper())
rvs = rvs.merge(state_info, on='state', how='inner')
rvs['county_state'] = rvs['county'].astype(str) + '_' + rvs['state_name']
rvs.index = rvs[['year', 'county', 'state_name']]
rvs = rvs[['rangeland_lb_acre']]

# Join Rangeland and Wheat Data
final_df = wheat_df.join(rvs,  how='outer')
final_df[['year', 'county', 'state']] = pd.DataFrame(final_df.index.tolist(),
                                                     index=final_df.index)
final_df = final_df.reset_index()

# This list will select what ever data items we chose from NASS
[items.insert(0, c) for c in ['rangeland_lb_acre', 'state', 'county', 'year']]
final_df = final_df[items]

# Save
final_df.to_csv('data/wheat_rvs.csv', na_rep='nan', index=False)
