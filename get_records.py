#!usr/bin/bash python
#
# How to use it:
# python3 get_records.py <path_to_NHANES_data> <file_with_codes> <output_csv_file>
#

import argparse, os, sys
import pandas as pd
from tqdm.auto import tqdm 

def main():
    
    def read_args():

        parser = argparse.ArgumentParser()

        parser.add_argument('path_to_NHANES_data', type=str,
            help='- the path with NHANES data, i.e., ./ for the current folder. \
            Expected to read nested folders with xz compressed CSV files')

        parser.add_argument('file_with_codes', type=str,
            help='- NHANES codes to collect. One code per line.')

        parser.add_argument('output_file', type=str,
            help='- CSV file with collected NHANES records.')

        args = parser.parse_args()

        return args.path_to_NHANES_data, args.file_with_codes, args.output_file

    def get_files(path, extension='xz'):

        files = []

        for _path, _subdirs, _files in os.walk(path):
            for _name in _files:
                files.append(os.path.join(_path, _name))

        return [file for file in files if file.split('.')[-1] == extension]

    def get_codes(file_with_codes):

        with open(file_with_codes) as f:
            lines = f.readlines()

        return list(map(lambda x: x.replace(' ', '\n').replace('#', '\n').split()[0], lines))

    # get all records of interest

    path_to_NHANES_data, file_with_codes, output_file = read_args()

    files = get_files(path_to_NHANES_data)

    codes_to_collect = get_codes(file_with_codes)

    df_all = pd.DataFrame(columns=['SEQN'])

    for idx, file_name in tqdm(enumerate(files), total=len(files)):

        df = pd.read_csv(file_name, low_memory=False, nrows=1)

        if 'SEQN' in df.columns:

            features = set(df.columns).intersection(set(codes_to_collect))
            
            df = pd.read_csv(file_name, low_memory=False, usecols=list(features) + ['SEQN'])
            
            df['SEQN'] = df['SEQN'].astype(int)

            if features:

                df_all = pd.merge(df[list(features) + ['SEQN']], 
                                  df_all, 
                                  suffixes=(f'_{idx}',''), 
                                  how = 'outer', 
                                  on ='SEQN')

    all_features = df_all.columns
    duplicated_features = set([i.split('_')[0] for i in all_features if '_' in i])
    unique_features = [i for i in all_features if not '_' in i]

    for feature in duplicated_features:
        duplicated = [_ for _ in all_features if feature in _]
        df_all[feature] = df_all[duplicated].fillna(method='ffill', axis=1).fillna(method='bfill', axis=1)[[feature]]

    # store result
    df_all[unique_features].set_index('SEQN').to_csv(output_file)

if __name__ == '__main__':
    main()
    sys.exit(0)
