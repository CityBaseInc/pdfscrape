'''
PDFPipeline is a class to manage the scraping and processing of PDF documents.

@author: Vidal Anguiano Jr.
'''

import urllib
import pandas as pd
import csv
import random
from src.pdf_utils import *
from datetime import datetime
import time
from src.db_conn import DB_Connection


class PDFPipeline(object):
	'''
	Simple pipeline for scraping PDFs from the internet given a list of PDF
	urls. Pipeline includes ability to load scraped data into a database.
	Specify the database to connect to with a "credentials.json".
	'''
	def __init__(self, pdf_urls_path, limit=None, creds = 'credentials.json'):
		self.pdfurls = load_pdf_urls(pdf_urls_path, limit=limit)
		self.num_pdfs = len(self.pdfurls)
		self.scrape_filename = None
		self.db_conn = DB_Connection(creds)


	def scrape_pdfs(self, maxpages, base, random_sample, sep = '`'):
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
		'''
		path = './data/temp/temp.pdf'
		pdfs = self.pdfurls[['pdf_id','pdf_url']].values.tolist()
		scrape_file = './data/scrapes/pdf_scrape_' + current_time_str() + '.csv'
		self.scrape_file = scrape_file
		with open(scrape_file, "w", newline='\n') as f:
			writer = csv.writer(f, delimiter = sep)
			writer.writerow(['pdf_id','pdf_url','dl_status','scrape_status',
							 'num_pages','num_pages_scraped','is_fillable',
							 'text'])
			for pdf in pdfs:
				pdf_id = pdf[0]
				url = pdf[1]
				print(url)
				try:
					dl_status = download_pdf(url, directory = './data/temp/',
												  temp = True)
					if dl_status == 'Success':
						fillable = is_fillable_pdf(path, maxpages)
						text, scrape_status = convert_pdf_to_txt(path, maxpages,
															base, random_sample)
						num_pages = pdf_num_pages(path = path)
						if num_pages < maxpages:
							num_pages_scraped = num_pages
						else:
							num_pages_scraped = maxpages
						writer.writerow([pdf_id, url, dl_status, scrape_status,
						 				 num_pages, num_pages_scraped, fillable,
										 text])
					else:
						writer.writerow([None]*8)

				except Exception as e:
					print(e)
					writer.writerow([pdf_id, url, 'Error', e])


	def load_in_db(self, table_name, sep = '`'):
		'''
		Load the exported scrape data into a database table.
		'''
		self.db_conn.create_table(csv_file = self.scrape_file,
								  table_name = table_name, sep = sep)
