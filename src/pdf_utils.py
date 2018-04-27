'''
PDFUtils is a set of functions for extracting text and other information from
PDF files.

@author: Vidal Anguiano Jr.
'''
import urllib
import pandas as pd
import os
from datetime import datetime
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.pdftypes import PDFObjRef
from io import StringIO
import random


def pdf_num_pages(open_pdf = None, path = None):
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

def convert_pdf_to_txt(path, maxpages = 0, sample_base = 0, random_sample_size = 0):
    '''
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

        if random_sample_size and numpages - sample_base > random_sample_size:
            pagenos = list(range(1, sample_base + 1))\
            + random.sample(k = random_sample_size,
                    population= range(sample_base +1,  numpages + 1))

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

def load_pdf_urls(filename, limit = None):
    '''
	Helper function for creating dataframe from a csv list of PDFs.
	'''
    pdf_links_df = pd.read_csv(filename)
    if limit:
        pdf_links_df = pdf_links_df.copy()[:limit]
    return pdf_links_df

def download_pdf(url, directory = './data/', temp = False):
    '''
    Downloads a pdf given a link into directory of choice.
    Inputs:
        - url (http url) url of pdf to download
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
            urllib.request.urlretrieve(url, directory + 'temp.pdf')
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

def download_and_scrape_pdf(url, maxpages=0, sample_base = 0, random_sample_size = 0):
    '''
    '''
    download_status = download_pdf(url, directory = './data/temp/', temp = True)
    print("Now on:", url)
    print("DL:", download_status)
    if download_status in ['Download: Failed', 'Not a PDF']:
        return download_status
    text = convert_pdf_to_txt('./data/temp/temp.pdf', maxpages, sample_base,
														random_sample_size)
    if text == 'Failed':
        print("Scrape: Failed")
        return None, 'Scrape: Failed'
    else:
        print("Scrape: Success")
        return text, 'Scrape: Success'

def is_fillable_page(page):
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


def current_time_str():
	now = datetime.now()
	current_time_list = [str(now.month), str(now.day),
						 str(now.hour), str(now.minute)]
    current_time = '_'.join(current_time_list)
    return current_time
