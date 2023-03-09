import asyncio

from scrape_glassdoor import *
from scrape_indeed import *

details_folder_prefix = "../dataset/details_dataset"
review_folder_prefix = "../dataset/reviews_dataset"

reviews_to_scrape = 500


def main():
    # asyncio.run(scrape_glassdoor())
    scrape_indeed()


if __name__ == "__main__":
    main()
