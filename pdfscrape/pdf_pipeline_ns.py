'''
@author: Vidal Anguiano Jr.
'''
import os
import urllib
import csv
import random
import time
from datetime import datetime

import pandas as pd

from pdfscrape import pdf_utils as pu


def scrape_pdfs(pdflink_q, maxpages, base, random_sample, to_scrape,
                scrape_file, temp_name, final=False, nlp=None):
    '''
    Downloads and scrapes information from each PDF, allowing to specify the
    max number of pages to scrape, how many to scrape from the beginning of
    the document, and how many pages to take as a random sample from the
    rest of the document. The data are exported as a csv with the following
    columns:
        - pdf_id - unique identifier for the pdf document
        - pdf_url - url for the PDF
        - dl_status - indicator of whether PDF downloaded successfully
        - scrape_status - indicator of whether the PDF scraped successfully
        - num_pages - number of pages in the entire document
        - num_pages_scraped - number of pages scraped from the document
        - is_fillable - indicates whether any pages in PDF are fillable
        - text - the text scraped from each of the scraped pages
        - nlp - named entities data

    Inputs:
        - pdflink_q: mp.Queue() object where pdf urls are pulled from
        - maxpages (int): maximum number of pages to be scraped
        - base (int): number of pages to be scraped from beginning
        - random_sample (int): scrapes random sample of pages from file
        - to_scrape (int): number of pdf files to scrape before while loop break
        - scrape_file (str): name of file to be written by resulting scrape
        - temp_name (str): name of temporary pdf file downloaded to be replaced
        - final (bool): flag for instructing whether pdflink_q should be purged
        - nlp: pycorenlp instance created to connect to Stanford CoreNLP server
    '''
    counter = 0
    sep = ','
    path = './data/temp/' + temp_name
    with open(scrape_file, "w", newline='\n') as f:
        writer = csv.writer(f, delimiter = sep)
        writer.writerow(['pdf_id',
                         'pdf_url',
                         'dl_status',
                         'scrape_status',
                         'num_pages',
                         'num_pages_scraped',
                         'is_fillable',
                         'text',
                         'nlp'])

        while True:
            if pdflink_q.qsize() != 0:
                url = pdflink_q.get()
                print("SCRAPING PDF URL:", url)
            else:
                continue
            pdf_id = counter
            url = pdflink_q.get()
            counter += 1

            try:
                dl_status = pu.download_pdf(url, directory = './data/temp/',
                                            temp = True, temp_name= temp_name)

                if dl_status == 'Success':
                    fillable = pu.is_fillable_pdf(path, maxpages)
                    text, scrape_status = pu.convert_pdf_to_txt(path, maxpages,
                                                                base, random_sample)
                    num_pages = pu.pdf_num_pages(path = path)
                    os.unlink(path)

                    if num_pages and num_pages < maxpages:
                        num_pages_scraped = num_pages

                    else:
                        num_pages_scraped = maxpages

                    try:
                        nlp_res = nlp.annotate(text, properties={'annotators': 'ner',
                                                                 'outputFormat': 'json'})
                        result = nlp_res['sentences'][0]['entitymentions']

                    except:
                        result = None

                    writer.writerow([pdf_id,
                                     url,
                                     dl_status,
                                     scrape_status,
                                     num_pages,
                                     num_pages_scraped,
                                     fillable,
                                     text,
                                     result])
                    print("SUCCESS")

                else:
                    writer.writerow([pdf_id,
                                     url,
                                     dl_status,
                                     None,
                                     None,
                                     None,
                                     None,
                                     None,
                                     None])
                    os.unlink(path)
                    print("FAILED")

            except Exception as e:
                print(e)
                writer.writerow([pdf_id, url, 'Error', e])
                print("FAILED")

            if counter >= to_scrape or (counter > 0 and pdflink_q.qsize() == 0):
                break

    if final:
        while not pdflink_q.empty():
            pdflink_q.get()
        print("pdflink_q is EMPTY.")



