import os
import shutil
from os import listdir
from os.path import isfile, join
from time import time
from multiprocessing import Pool
from pdfscrape import pdf_utils as pu
from pdfscrape.db_conn import DBConnection
from pdfscrape import text_processing as tp
from pdfscrape.pdf_pipeline import PDFPipeline

def load_data(db_conn):
    db_conn.connect()
    cur = db_conn.conn.cursor()
    cur.execute('''
                DROP TABLE IF EXISTS chicago_pdfs;''')
    db.conn.commit()
    db_conn.create_table('data/all_Chicago_pdfs_nodups.csv', 'chicago_pdfs')

def multi_pdfscrape(table):
    # l = ["*{maxpages=10, base=5, random_sample_size=5,sep='`'}"]

    pdfp = PDFPipeline(table, creds = 'creds.json')
    pdfp.scrape_pdfs(10, 5, 5, sep='`', multi=True)

def delete_dl_files():
    temp_folder = './data/temp'

    for file in listdir(temp_folder):
        file_path = join(temp_folder, file)
        os.unlink(file_path)

def combine_csvs(filename, files):
    fout=open(filename, "a")
    # first file:
    for line in open(files[0]):
        fout.write(line)
    # now the rest:
    for file in files[1:]:
        print(file)
        f = open(file, "r+")
        lines = f.readlines()[1:]
        # f.next() # skip the header
        for line in lines:
             fout.write(line)
        f.close() # not really needed
    fout.close()


if __name__ == "__main__":
    db = DBConnection('creds.json')
    db.connect()
    pdfs = db.query('''SELECT * FROM chicago_pdfs;''')
    tables = []
    for x in range(0,40):
        sample = pdfs.sample(50)
        table_name = 'sample_' + str(x)
        db.create_table_from_df(sample, table_name, override_edits = True)
        tables.append(table_name)

    p = Pool(40)
    start = time()
    time_ = pu.current_time_str()
    folder_name = 'data/scrapes/multi_run_' + time_

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    p.map(multi_pdfscrape, tables)
    p.terminate()
    p.join()
    print('TIME:', time() - start, 's')

    files = [join(folder_name,f) for f in listdir(folder_name)]
    multi_filename = folder_name + '/multi_' + time_ + '_combined.csv'

    combine_csvs(multi_filename, files)
    delete_dl_files()






    # load_data(db)
