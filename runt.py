from pdfscrape.pdf_pipeline import PDFPipeline

def runt(table):
    # l = ["*{maxpages=10, base=5, random_sample_size=5,sep='`'}"]
    pdfp = PDFPipeline(table)
    pdfp.scrape_pdfs(10, 5, 5, sep='`', multi=True)
