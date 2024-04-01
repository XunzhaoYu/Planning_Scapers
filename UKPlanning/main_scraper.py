from scrapy import cmdline
import sys
    
if __name__=="__main__":
    cmdline.execute(f"scrapy crawl UKPlanning_Scraper -L WARNING -a auth_index={sys.argv[1]} -a year={sys.argv[2]}".split())
