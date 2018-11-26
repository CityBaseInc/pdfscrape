'''
PDFUtils is a set of functions for extracting text and other information from
PDF files.

@author: Vidal Anguiano Jr.
'''
import os
from io import StringIO
import random
import urllib
from datetime import datetime

import pandas as pd
import timeout_decorator
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.pdftypes import PDFObjRef


def pdf_num_pages(open_pdf = None, path = None):
	'''
	Get the number of pages in the PDF document.
	'''
	try:
		if path:
			open_pdf = open(path, 'rb')
		parser = PDFParser(open_pdf)
		doc = PDFDocument(parser)
		numpages = doc.catalog['Pages'].resolve()['Count']
		return numpages
	except Exception as e:
		print(e)
		return None


@timeout_decorator.timeout(20) # Interrupts function if execution time > 20 sec
def convert_pdf_to_txt(path,
                       maxpages = 0,
                       base = 0,
                       random_sample_size = 0):
    '''
	Scrape text data from a PDF with the ability to set a max number of pages,
	and for larger documents, setting the number of pages to take from the
	beginning of the document and the option to take a sample set from the
	remaining pages.

    Inputs:
        - path (str): path of the pdf to be scraped
        - maxpages (int): maximum number of pages to be scraped
        - base (int): number of pages to be scraped from beginning
        - random_sample_size (int): scrapes random sample of pages from file

    Returns:
        - Scraped text along with scrape status, "Success" or "Failed"
	'''
    try:
        rsrcmgr = PDFResourceManager()
        retstr = StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
        fp = open(path, 'rb')
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        password = ""
        caching = True
        pagenos = set()
        numpages = pdf_num_pages(fp)

        if random_sample_size and numpages - base > random_sample_size:
            pagenos = list(range(1, base + 1))\
            + random.sample(k = random_sample_size,
                    population= range(base +1,  numpages + 1))

        for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages,
					password=password, caching=caching, check_extractable=True):
            interpreter.process_page(page)

        text = retstr.getvalue()

        fp.close()
        device.close()
        retstr.close()
        return text.replace('\n',' '), "Success"
    except Exception as e:
        print(e)
        return None, "Failed"


def download_pdf(url,
                 directory = './data/',
                 temp = False,
                 temp_name = 'temp.pdf'):
    '''
    Downloads a pdf given a link into directory of choice.
    Inputs:
        - url (str): url of pdf to download
        - directory (str): directory where file should be saved
        - temp (bool): flag for whether file should be given temporary name
          so that it may be overwritten by the next file to be downloaded
        - temp_name (str): name to be given to the temp file
    Outputs:
        - Downloads file (.pdf) file downloaded into directory given
        - Success/Fail (string) indicates whether download succeeded or failed
    '''
    if '.pdf' not in url:
        return 'Not a PDF'
    url = url.replace(' ', '%20')

    try:

        if os.path.exists(directory) == False:
            os.mkdir(directory)

        if temp:
            urllib.request.urlretrieve(url, directory + temp_name)

        else:
            parsed_url = urllib.parse.urlparse(url)
            path = parsed_url.path
            split_path = path.split('/')
            file_name = split_path[-1]
            file_name = file_name.replace('%20','_')
            urllib.request.urlretrieve(url, directory + file_name)

        return 'Success'

    except Exception as e:
        print(e)
        return 'Failed'


def download_and_scrape_pdf(url,maxpages=0,
                            sample_base = 0,
                            random_sample_size = 0):
    '''
	Composite function joining the ability to download and scrape a PDF in one
	command.

    '''
    download_status = download_pdf(url, directory = '../data/temp/', temp = True)
    print("Now on:", url)
    print("DL:", download_status)
    if download_status in ['Download: Failed', 'Not a PDF']:
        return download_status
    text = convert_pdf_to_txt('../data/temp/temp.pdf', maxpages, sample_base,
														random_sample_size)
    if text == 'Failed':
        print("Scrape: Failed")
        return None, 'Scrape: Failed'
    else:
        print("Scrape: Success")
        return text, 'Scrape: Success'


def is_fillable_page(page):
	'''
	Helper function used to indicate whether a page is fillable.
	'''
	assert isinstance( page, PDFPage ), 'This is not a PDFPage.'
	if isinstance( page.annots, PDFObjRef ):
		for annot in page.annots.resolve():
			if isinstance( annot, PDFObjRef ):
				annot = annot.resolve()
				if 'Subtype' in annot and annot['Subtype'].name == 'Widget' and\
				                          annot['FT'].name in ['Tx','Ch','Btn']:
					return True
				else:
					return False
			else:
				raise Exception( "Unknown Object")
	return False


def is_fillable_pdf(path, maxpages = 0):
	'''
	Takes a PDF and indicates whether the scraped pages are fillable. Set
	maxpages to check the first couple of pages from the beginning of the PDF.
	'''
	fp = open(path, 'rb')
	try:
		pages = list(PDFPage.get_pages(fp, maxpages = maxpages))
		for page in pages:
			if is_fillable_page(page) == True:
				fp.close()
				return "Fillable PDF."
		fp.close()
		return "Not a fillable PDF."
	except Exception as e:
		print(e)
		return "Fillable Check: Failed"


def current_time_str(extended=False):
	'''
	Helper function to get the time and date, used for filenames to prevent
	overwriting.
	'''
	now = datetime.now()
	if extended:
		current_time_list = [str(now.month), str(now.day),
							 str(now.hour), str(now.minute),
							 str(now.second), str(now.microsecond)]
	else:
		current_time_list = [str(now.month), str(now.day),
						 str(now.hour), str(now.minute)]
	current_time = '_'.join(current_time_list)
	return current_time
