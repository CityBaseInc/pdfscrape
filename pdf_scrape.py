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
import interruptingcow
import random
import time

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

        for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching, check_extractable=True):
            interpreter.process_page(page)

        text = retstr.getvalue()

        fp.close()
        device.close()
        retstr.close()
        return text
    except Exception as e:
        print(e)
        return 'Failed.'

def pdf_file_to_df(filename):
	'''
	Helper function for creating dataframe from a csv list of PDFs.
	'''
	pdf_links_df = pd.read_csv(filename)
	return pdf_links_df

def download_pdf(url, directory = './', temp = False):
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
    download_status = download_pdf(url, directory = './temp/', temp = True)
    print("Now on:", url)
    print("DL:", download_status)
    if download_status in ['Failed', 'Not a PDF']:
        return 'Download Failed'
    text = convert_pdf_to_txt('./temp/temp.pdf', maxpages, sample_base, random_sample_size)
    if text == 'Failed':
        print("Scrape: Failed")
        return 'Scrape Failed'
    else:
        print("Scrape: Success")
        return text

def is_fillable_page(page):
    assert isinstance( page, PDFPage ), 'This is not a PDFPage.'
    if isinstance( page.annots, PDFObjRef ):
        for annot in page.annots.resolve():
            if isinstance( annot, PDFObjRef ):
                annot = annot.resolve()
                if 'Subtype' in annot and annot['Subtype'].name == 'Widget' and annot['FT'].name in ['Tx','Ch','Btn']:
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
        return "Failed"


def assess_pdfs(df, column, maxpages = 0, sample_base = 0, random_sample_size = 0):
    dl_statuses = []
    fillable = []
    pdf_text = []
    num_pages = []

    csv = open('pdf_scrape_' + current_time_str() + '.csv', "w")
    for url in df[column]:
        print(url)
        status = download_pdf(url, directory = './temp/', temp = True) 
        dl_statuses.append(status)
        if status == 'Success':
            fillable.append(is_fillable_pdf('temp/temp.pdf', maxpages))
            pdf_text.append(convert_pdf_to_txt('temp/temp.pdf', maxpages, sample_base, random_sample_size))
            num_pages.append(pdf_num_pages(path = 'temp/temp.pdf'))
        else:
            fillable.append(None)
            pdf_text.append(None)
            num_pages.append(None)
    df['dl_statuses'] = dl_statuses
    df['fillable'] = fillable
    df['pdf_text'] = pdf_text
    df['num_pages'] = num_pages
    return df


def current_time_str():
    current_time_list = []
    current_time_list.append(str(datetime.now().month))
    current_time_list.append(str(datetime.now().day))
    current_time_list.append(str(datetime.now().hour))
    current_time_list.append(str(datetime.now().minute))
    current_time = '_'.join(current_time_list)
    return current_time