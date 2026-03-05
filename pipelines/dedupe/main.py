import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import numpy as np
import dedupe
import re
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field
import os

class Settings(BaseSettings):
    db_user: str
    db_host: str
    db_name: str
    db_pass: str
    db_port: str

    @computed_field
    @property
    def db_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

# Residential Land Use Codes from MA Dept of Revenue
# https://www.mass.gov/files/documents/2016/08/wr/classificationcodebook.pdf
# Codes are 101*-109*, 031*, and 013*
# Often include suffixes (letters, zeroes or no character), thus regex *?
USE_CODES = '^1[0-1][1-9]*?|^013*?|^031*?'
BOS_CODES = '^R[1-4]$|^RC$|^RL$|^CD$|^A$'
SETTINGS = 'training/learned_settings'
TRAINING = 'training/training.json'
PARCELS_FILE = 'data/parcels/mamas_parcels.shp'

def read_res(file, name):
    df = gpd.read_file(file).drop('geometry', axis='columns')
    df['town'] = name
    return df

def tupleize(row):
    if (row['co'] is not None) & (row['own'] is not None):
        return tuple([row['own'], row['co']])
    elif (row['co'] is None) & (row['own'] is not None):
        return tuple([row['own']])
    else:
        return None

def process_somerville(file='data/assess/som_assess_FY14-FY19.csv', mg_file='data/assess/som_massgis.dbf'):
    # Somerville processing.
    df = pd.read_csv(file,
                    dtype={'HOUSE NO': str}) 
    df.columns = df.columns.str.lower()

    # Filter for residential parcels.
    df = df[df['pcc'].str.contains(USE_CODES, regex=True)]

    df.loc[:,'prop_addr'] = [' '.join((str(a), str(b))) for a, b in zip(df['house no'], df['street'])]

    df = df.replace({r'^, ': '', r' ,': '', r', nan': '', r'nan': '', r'None, ': '', r', None': ''}, regex=True)
    df = df.replace({' ': None, '': None, np.nan: None})

    # Pad ZIP code with zeroes, remove 4-digit suffix.
    # Assessor appears to have overzealously corrected...
    df.loc[:,'owner zip'] = df['owner zip'].str[1:]
    df.loc[:,'own_addr'] = [', '.join((str(a), str(b), str(c), str(d))) for a, b, c, d in zip(df['owner add'], df['owner city'], df['owner state'], df['owner zip'])]

    df.loc[:,'gisid'] = ['-'.join((str(m), str(b), str(l))) for m, b, l in zip(df['map'], df['block'], df['lot'])]
    df['town'] = 'som'
    df = df.drop(['year'], axis=1)
    df = df.rename(columns = {
        'commitment owner': 'own',
        'current co-owner': 'co',
        'parcel val': 'ass_val',
        'fiscal_year': 'year'
    })

    # Assessor seems to have screwed up this column in the 2014-2019 data
    # but it appears that 2019 data is incrementally numbered (¯\_(ツ)_/¯)
    df = df.loc[df['year'] >= 2019]
    df['year'] = 'FY2019'
    # Filter columns.
    df = df[['gisid', 'town', 'prop_addr', 'unit', 'own', 'co', 'own_addr', 'ass_val', 'year']]

    # Somerville 2019 assessor's table doesn't include sale date 
    # (apparently by accident), so we collate with MassGIS source.
    df_mg = read_res(mg_file, "som")
    # Rename column to lower-case.
    df_mg.columns = df_mg.columns.str.lower()
    # Remame columns
    df_mg = df_mg.rename(columns = {
        'prop_id': 'gisid',
        'ls_date': 'sale_d',
        'ls_price': 'sale_p'
    })
    df_mg.loc[:,'sale_d'] = pd.to_datetime(df_mg['sale_d'], format='%Y%m%d')
    # Replace underscores with hyphens.
    df_mg.loc[:,'gisid'] = df_mg.gisid.str.replace(r'_', '-', regex=True)
    df_mg.loc[:,'sale_p'] = df_mg['sale_p'].replace(0, None)
    # Filter columns.
    df_mg = df_mg[['gisid', 'sale_d', 'sale_p']]
    df = df.merge(df_mg[['gisid', 'sale_d', 'sale_p']], how='left', on='gisid')
    return df

def process_medford(file='data/assess/med_massgis.dbf'):
    df = read_res(file, "med")
    # Rename column to lower-case.
    df.columns = df.columns.str.lower()
    # Filter for residential paWorldrcels.
    df = df[df['use_code'].str.contains(USE_CODES, regex=True)]
    # Identify rows with co-owner names erroneously listed in address column.
    mask = df.own_addr.str.contains(pat = '|'.join(['^C/O', '^[A-Za-z]']), na=False) & ~df.own_addr.str.contains(pat = '|'.join(['^PO', '^P.O.', '^P. O.', '^P O ', '^ONE', '^BOX', '^ZERO']), na=False)
    # Add co-owners identified to co column.
    df['co'] = df.own_addr[mask]
    df.loc[~mask, 'co'] = None
    # Fill own_addr with none for above-identified rows.
    df.loc[mask, 'own_addr'] = None
    # Remame columns
    df = df.rename(columns = {
        'prop_id': 'gisid',
        'owner1': 'own',
        'site_addr': 'prop_addr',
        'total_val': 'ass_val',
        'location': 'unit',
        'ls_date': 'sale_d',
        'ls_price': 'sale_p'
    })
    df.loc[:,'sale_d'] = pd.to_datetime(df['sale_d'], format='%Y%m%d')
    df.loc[:,'prop_addr'] = df.prop_addr.str.strip()
    # Replace underscores with hyphens.
    df.loc[:,'gisid'] = df.gisid.str.replace(r'_', '-', regex=True)
    # Concatenate address.
    df.loc[:,'own_addr'] = [', '.join((str(a), str(b), str(c))) for a, b, c in zip(df['own_addr'], df['own_city'], df['own_state'])]
    df.loc[:,'own_addr'] = [' '.join((str(a), str(b))) for a, b in zip(df['own_addr'], df['own_zip'])]
    # Remove concatenated Nones.
    df = df.replace({r'None, ': ''}, regex=True)
    df['year'] = 'FY2019'
    df.loc[:,'sale_p'] = df['sale_p'].replace(0, None)
    # Filter columns.
    return df[['gisid', 'town', 'prop_addr', 'unit', 'own', 'co', 'own_addr', 'ass_val', 'year', 'sale_d', 'sale_p']]

def process_cambridge(file='data/assess/cam_assess.csv'):
    df = pd.read_csv(file,
                  parse_dates=['SaleDate'],
                  dtype={'Owner_Zip': str, 
                         'SalePrice': float,
                         'StateClassCode': str
                        })
    # rename all columns to lowercase
    df.columns = df.columns.str.lower()
    # Filter for residential properties.
    df = df[df['stateclasscode'].str.contains(USE_CODES, regex=True)]
    # Pad zip to five digits and remove 4-digit zip suffix.
    df.loc[:,'owner_zip'] = df['owner_zip'].str.rsplit('-', 1).str[0]
    # Identify rows with co-owner names erroneously listed in address column.
    mask = df.owner_address.str.contains(pat = '|'.join(['^C/O', '^ATTN:']), na=False)
    df.loc[mask, 'owner_address'] = None
    # Add co-owners identified to co column.
    df.loc[mask, 'owner_coownername'] = [', '.join((str(a), str(b)))  for a, b in zip(df.loc[mask, 'owner_coownername'], df.loc[mask, 'owner_address'])]
    # Concatenate owner address components
    df.loc[:,'own_addr'] = [', '.join((str(a), str(b), str(c), str(d))) for a, b, c, d in zip(df['owner_address'], df['owner_address2'], df['owner_city'], df['owner_state'])]
    df.loc[:,'own_addr'] = [' '.join((str(a), str(b))) for a, b in zip(df['own_addr'], df['owner_zip'])]
    df.loc[:,'own_addr'] = df.own_addr.str.strip()
    # Clean property address column
    df['prop_addr'] = df['address'].str.rsplit('\ndfbridge, MA', 1).apply(lambda x: x[0].replace('\n', ' ').strip())
    # Bring property address in line with others.
    df['town'] = 'cam' 
    df = df.rename(columns = {
        'owner_name': 'own',
        'owner_coownername': 'co',
        'assessedvalue': 'ass_val',
        'saleprice': 'sale_p',
        'saledate': 'sale_d'
    })
    df['year'] = 'FY2020'
    df['sale_p'].values[df['sale_p'].values < 1] = None
    df = df.replace({r'^, ': '', r' ,': '', r', nan': '', r'None, ': '', r', None': ''}, regex=True)
    df = df.replace({' ': None, '': None, np.nan: None})
    return df[['gisid', 'town', 'prop_addr', 'unit', 'own', 'co', 'own_addr', 'ass_val', 'year', 'sale_d', 'sale_p']]

def process_boston(file='data/assess/bos_assess.csv'):
    df = pd.read_csv(file, dtype={'GIS_ID': str, 'MAIL_ZIPCODE': str, 'U_TOT_RMS': str})
    df.columns = df.columns.str.lower()
    df = df.rename(columns = {
        'gis_id': 'gisid',
        'owner': 'own',
        'mail_addressee': 'co',
        'unit_num': 'unit',
        'av_total': 'ass_val'
    })
    df['town'] = 'bos'
    # Filter by residential property types.
    df = df[df['lu'].str.contains(BOS_CODES, regex=True)]
    df.loc[:, 'gisid'] = df.gisid.str.strip().str.pad(width=10, side='left', fillchar='0')
    # Pad ZIP code with zeroes, remove 4-digit suffix.
    df.loc[:,'mail_zipcode'] = df.mail_zipcode.astype(str).str.strip().str.pad(width=5, side='left', fillchar='0')
    # Add comma between city and state.
    df.loc[:,'mail cs'] = df['mail cs'].str.rsplit(' ', 1).apply(lambda x: ', '.join(x))
    # Concatenate property address components
    df.loc[:,'prop_addr'] = [' '.join((str(a), str(b), str(c))) for a, b, c in zip(df['st_num'], df['st_name'], df['st_name_suf'])]
    df.loc[:,'prop_addr'] = df.prop_addr.str.strip()
    # Concatenate owner address components.
    df.loc[:,'own_addr'] = [', '.join((str(a), str(b))) for a, b in zip(df['mail_address'], df['mail cs'])]
    df.loc[:,'own_addr'] = [' '.join((str(a), str(b))) for a, b in zip(df['own_addr'], df['mail_zipcode'])]
    df.loc[:,'own_addr'] = df.own_addr.str.strip()
    # Filter columns
    df['year'] = 'FY2020'
    df = df[['gisid', 'town', 'prop_addr', 'unit', 'own', 'co', 'own_addr', 'ass_val', 'year']]
    # Replace blank strings with None (necessary for dedupe).
    df = df.replace({' ': None, '': None, r' #nan': None})
    df = df.replace({r' #nan': ''}, regex=True)
    return df

def process_brookline(file='data/assess/brook_assess.csv'):
    df = pd.read_csv(file, 
                    dtype={'SALEPRICE': float,
                          'USECD': str},
                    parse_dates=['SALEDATE'])
    df.columns = df.columns.str.lower()
    df = df[df['usecd'].str.contains(USE_CODES, regex=True)]

    df.loc[:,'zip'] = df['zip'].str.rsplit('-', 1).str[0]
    # Name town.
    df['town'] = 'brk' 
    # Concatenate address.
    df.loc[:,'own_addr'] = [', '.join((str(a), str(b), str(c))) for a, b, c, in zip(df['address'], df['city'], df['state'])]
    # Append zip to address with no comma.
    df.loc[:,'own_addr'] = [' '.join((str(a), str(b))) for a, b in zip(df['own_addr'], df['zip'])]
    df.loc[:,'own_addr'] = df.own_addr.str.strip()
    # Concatenate property address components
    df.loc[:,'prop_addr'] = [''.join((str(a), str(b))) for a, b in zip(df['addno1'], df['addno2'])]
    df.loc[:,'prop_addr'] = [' '.join((str(a), str(b))) for a, b in zip(df['prop_addr'], df['addst1'])]
    df.loc[:,'prop_addr'] = df.prop_addr.str.strip()
    # Append 
    df.loc[:,'own'] = [' '.join((str (a), str(b))) for a, b in zip(df['firstname1'], df['lastname1'])]
    df.loc[:,'co'] = [' '.join((str(a), str(b))) for a, b in zip(df['firstname2'], df['lastname2'])]
    df = df.replace({' ': None, '': None})
    df = df.rename(columns = {
        'parcel-id': 'gisid',
        'addst2': 'unit',
        'restotlval': 'ass_val',
        'saleprice': 'sale_p',
        'saledate': 'sale_d'
    })
    df = df.replace({r'^, ': '', r' ,': '', r', nan': '', r'nan': '', r'None, ': '', r', None': ''}, regex=True)
    df = df.replace({' ': None, '': None})
    df['sale_p'].values[df['sale_p'].values < 1] = None
    df['year'] = 'FY2020'
    return df[['gisid', 'town', 'prop_addr', 'unit', 'own', 'co', 'own_addr', 'ass_val', 'year', 'sale_d', 'sale_p']]

def main():
    settings = Settings()
    all_assess = pd.concat([
        process_somerville(), 
        process_medford(), 
        process_cambridge(), 
        process_boston(), 
        process_brookline()
        ], ignore_index=True)
    all_assess.loc[:,'prop_addr'] = all_assess.prop_addr.str.lstrip('0').str.strip()
    all_assess.loc[:,'own_addr'] = all_assess.own_addr.str.lstrip('0').str.strip()
    all_assess.loc[:,'co'] = all_assess['co'].replace({r'C/O ': '', r'S/O ': '', r'ATTN: ': '', r'ATTN ': ''}, regex=True)
    all_assess = all_assess.replace({r'None': '', 'nan': ''}, regex=True)
    all_assess = all_assess.replace({' ': None, '': None})
    all_assess = all_assess[~pd.isnull(all_assess['gisid'])]
    all_assess = all_assess.replace({pd.np.nan: None})
    all_assess['owners'] = all_assess.apply(tupleize, axis=1)

    # Convert to dictionary (expected by Dedupe)
    all_assess_dict = all_assess[['owners','own_addr']].to_dict('index')

    # If settings exist, read from existing.
    if os.path.exists(SETTINGS):
        print('Reading learned settings from', SETTINGS)
        with open(SETTINGS, 'rb') as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        # Tell Dedupe which fields are used to identify duplicates.
        fields = [
            {'field': 'owners', 'variable name': 'owners', 'type': 'Set'},
            {'field': 'own_addr', 'variable name': 'own_addr', 'type': 'Address'}
            ]
        deduper = dedupe.Dedupe(fields)
        # If training file exists, read it...
        if os.path.exists(TRAINING):
            print('reading labeled examples from ', TRAINING)
            with open(TRAINING, 'rb') as f:
                deduper.prepare_training(all_assess_dict, f)
        # Otherwise, prepare a training set...
        else:
            deduper.prepare_training(all_assess_dict)
        # Start supervised labeling.
        dedupe.console_label(deduper)
        deduper.train()
        # Write settings and training sets.
        with open(TRAINING, 'w') as tf:
            deduper.write_training(tf)
        with open(SETTINGSe, 'wb') as sf:
            deduper.write_settings(sf)
    
    # Identify clusters based on training dataset.
    # Higher threshold is less tolerant of differences between names/addresses.
    clustered_dupes = deduper.partition(all_assess_dict, threshold = 0.5)

    # How many sets are there?
    print('Number of sets', len(clustered_dupes))

    # Create empty arrays to hold results.
    rid = []
    clst = []
    conf = []
    count = []

    # Iterate over results...
    for cluster_id, (records, scores) in enumerate(clustered_dupes):
        for record_id, score in zip(records, scores):
            # Append record id
            # Corresponds to index of assessor dataframe.
            rid.append(record_id)
            # Append cluster ID.
            clst.append(cluster_id)
            # Append confidence score.
            conf.append(score)

    # Build new dataframe using result arrays.
    clust = pd.DataFrame(list(zip(clst, conf)), 
                    columns =['clst', 'conf'],
                    index = rid
                    )

    # Join clusters to assessors dataframe.
    all_assess = all_assess.join(clust)

    # Read spatial data
    parcels_gdf = gpd.read_file(PARCELS_FILE)
    parcels_gdf = parcels_gdf.rename(columns = {
        'pid': 'gisid'
    }).drop_duplicates(subset=['gisid', 'town'])
    parcels_gdf = parcels_gdf[~pd.isnull(parcels_gdf['gisid'])]
    parcels_gdf = parcels_gdf[~pd.isnull(parcels_gdf['geometry'])]
    # parcels_gdf.loc[:,'geometry'] = parcels_gdf.geometry.centroid
    centroid = parcels_gdf.geometry.centroid
    parcels_gdf.loc[:,'lat'] = centroid.y
    parcels_gdf.loc[:,'lon'] = centroid.x

    all_assess = parcels_gdf.merge(all_assess, on=['town', 'gisid'], how='right')
    all_assess = all_assess[~np.isnan(all_assess.lat)]

    # Hard-coding this count logic saves a ton of time for each PostgreSQL query.
    all_assess = all_assess.merge(all_assess.groupby('clst').count()[['gisid']].rename(columns={'gisid': 'count'}), on=['clst', 'clst'], how='left')

    pg_engine = create_engine(settings.db_url)
    all_assess.to_postgis("props", con=pg_engine, schema='public', if_exists='replace', index=True, index_label='id')
    

if __name__ == "__main__":
    main()
