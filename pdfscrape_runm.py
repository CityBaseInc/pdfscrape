from multiprocessing import Pool
from runt import runt

if __name__ == "__main__":
    p = Pool(2)
    l = ['pdfs_small1', 'pdfs_small2']
    p.map(runt, l)
    p.terminate()
    p.join()
