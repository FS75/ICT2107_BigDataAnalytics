import asyncio

#from scrape_glassdoor import *
#from scrape_glassdoorv2 import *
# from scrape_indeed import *
from scrape_indeedv2 import *


details_folder_prefix = "../../dataset/details_dataset"
review_folder_prefix = "../../dataset/reviews_dataset"

reviews_to_scrape = 100000


def main():
    with open('companies_to_scrape.txt', 'r') as companies:
        scrape_indeedv2(companies, reviews_to_scrape)
    #test_scrape()
    #asyncio.run(scrape_glassdoor())
    #scrape_indeed()
    #test_scrape2()

#def test_scrape():
    #scrape_indeedv2(["apple"], 10)
    #scrape_glassdoorv2(["apple"], 10)
#    asyncio.run(scrape_glassdoor())




if __name__ == "__main__":
    main()
