import sys
import pandas as pd
from pdfscrape.pdf_pipeline import PDFPipeline
from pdfscrape.db_conn import DBConnection
from multiprocessing import Pool

if __name__ == "__main__":
    pdfp = PDFPipeline('pdfs_sample')
    pdfp.scrape_pdfs(10, 5, 5, sep='`')
