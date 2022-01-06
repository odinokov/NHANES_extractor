#!usr/bin/bash python
#
# download all NHANES datasets and store them as xz compressed CSV files
#
# How to use it:
# python3 download.py out
#
# Install some libratries if needed, i.e.
# conda install -c conda-forge pyreadstat requests beautifulsoup4

import pandas as pd
from tqdm.auto import tqdm
from bs4 import BeautifulSoup
import time, tempfile, requests, urllib, os, sys, itertools, argparse, pyreadstat, shutil, itertools
from multiprocessing.pool import ThreadPool, Pool

def main():
    
    def read_args():

        parser = argparse.ArgumentParser()

        parser.add_argument('output', type=str, help='- an output directory')

        args = parser.parse_args()

        path = os.path.join(os.getcwd(), args.output)

        access_rights = 0o755

        try:
            os.makedirs(path, access_rights)
        except OSError as error:
            sys.exit(f'Creation of the directory {path} failed because {error.args[-1]}')

        else:
            print(f'Successfully created the directory {path}')

        return path

    def pool_factory(key, n):
        if key == 'proc':
            return Pool(n)
        elif key == 'thread':
            return ThreadPool(n)
        
    def parallel_run(func, args, CPU=4):
        with pool_factory('thread', CPU) as pool:
            results = list(tqdm(pool.imap_unordered(func, args), 
                                total=len(args), 
                                mininterval=1))
        pool.close(); pool.join()
        
        return resluts

    def get_urls(args):

        cycle, comp = args

        tqdm.write(f'Getting URLs for cycle {cycle}: {comp}...')

        begin_year, end_year = cycle.split('-')

        url = f'https://wwwn.cdc.gov/nchs/nhanes/Search/DataPage.aspx?Component={comp}&CycleBeginYear={begin_year}&CycleEndYear={end_year}'

        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

        content = requests.get(url, headers=headers).content
        soup = BeautifulSoup(content, features='lxml')
        cdc_table = soup.find('table')
        all_links = map(lambda _: _['href'], cdc_table.find_all('a', href=True))
        XPT_urls = filter(lambda _: _.split('.')[-1] == 'XPT', all_links)    

        return list(XPT_urls)

    def get_all_urls():

        NHANES_releases = ['2017-2020', '2017-2018', '2015-2016', '2013-2014', 
                           '2011-2012', '2009-2012', '2009-2010', '2007-2014', 
                           '2007-2012', '2007-2008', '2005-2006', '2003-2006', 
                           '2003-2004', '2001-2002', '2000-2004', '1999-2018', 
                           '1999-2016', '1999-2006', '1999-2004', '1999-2000']

        COMPONENTS = ['Demographics', 'Dietary', 'Examination', 'Laboratory', 'Questionnaire']

        cycle_component = list(itertools.product(NHANES_releases, COMPONENTS))
        
        urls = set(itertools.chain(*parallel_run(get_urls, cycle_component)))

        return urls

    def get_file(args):

        path, url = args

        with tempfile.TemporaryDirectory(prefix='tmp_XPT', dir='./') as tmpdirname:

            basename = os.path.basename(url)
            tmp_file_name = os.path.join(tmpdirname, basename)
            csv_file_name = os.path.join(path, os.path.basename(url).split('.')[0] + '.csv')

            # download and store XPT file
            with requests.get(f'https://wwwn.cdc.gov{url}', stream=True) as r:
    #             r.raise_for_status()
                with open(tmp_file_name, 'wb') as f:
                    shutil.copyfileobj(r.raw, f, length=16*1024*1024)

            # read XPT file and store it as zx compressed CSV
            pyreadstat.read_xport(tmp_file_name, encoding='windows-1252')[0].to_csv(
                f'{csv_file_name}.xz', index=None, compression='xz')
            
    # main

    path, urls = read_args(), get_all_urls()

    print(f'Going to download {len(urls):,} files. Please wait...', file=sys.stdout)
    
    _ = parallel_run(get_file, list(itertools.zip_longest([path], urls, fillvalue=path)))

if __name__ == '__main__':
    start_time = time.time()
    main()
    sys.exit(f'--- Completed in {time.time() - start_time:,.2f} seconds ---')
