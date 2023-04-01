import asyncio

from scrape_glassdoor import *
from scrape_indeedv2 import *

review_folder_prefix = "../../Dataset_used/ReviewsDataset/"

reviews_to_scrape = 100000


def main():
    with open('companies_to_scrape.txt', 'r') as companies:
        scrape_indeedv2(companies, reviews_to_scrape)
    asyncio.run(scrape_glassdoor())
    # test_scrape()
    # scrape_indeed()
    # test_scrape2()


# def test_scrape():
# scrape_indeedv2(["apple"], 10)
# scrape_glassdoorv2(["apple"], 10)
#    asyncio.run(scrape_glassdoor())

if __name__ == "__main__":
    main()
