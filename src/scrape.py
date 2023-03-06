import asyncio
import httpx
import json
import re
from typing import Tuple, List, Dict
from parsel import Selector
import pandas as pd

country_code = {
    "Argentina": 13,
    "Australia": 5,
    "Belgique (Français)": 15,
    "België (Nederlands)": 14,
    "Brasil": 9,
    "Canada (English)": 3,
    "Canada (Français)": 19,
    "Deutschland": 7,
    "España": 8,
    "France": 6,
    "Hong Kong": 20,
    "India": 4,
    "Ireland": 18,
    "Italia": 23,
    "México": 12,
    "Nederland": 10,
    "New Zealand": 21,
    "Schweiz (Deutsch)": 16,
    "Singapore": 22,
    "Suisse (Français)": 17,
    "United Kingdom": 2,
    "United States": 1,
    "Österreich": 11
}

companies_to_scrape = ["meta", "amazon", "apple", "netflix", "google"]

pandas_dict = {
    "employer_name": [],
    "employer_id": [],
    "employer_website": [],
    "employer_description": [],
    "employer_size": [],
    "employer_revenue": [],
    "employer_headquarters": [],
    "employer_founded": [],
    "employer_industry": []
}

# define async for review scrape
def scrape_glassdoor():
    for company in companies_to_scrape:
        print(f"Scraping {company}'s details from Glassdoor...")
        company_payload = find_companies(company)
        response = get_response(f"https://www.glassdoor.com/Overview/Working-at-"
                                f"{company_payload['suggestion']}-EI_IE"
                                f"{company_payload['id']}.htm")
        glassdoor_payload = get_glassdoor_payload(company_payload, response)
        payload_to_pandas_dict(glassdoor_payload)
        pandas_dict_to_csv('../scrape_dataset/glassdoor.csv')

    # review scrape not working
    # async with httpx.AsyncClient(
    #         timeout=httpx.Timeout(30.0),
    #         cookies={"tldp": "1"},
    #         follow_redirects=True,
    # ) as client:
    #     reviews = await scrape_reviews("eBay", "7853", client)
    #     print(json.dumps(reviews, indent=2))


"""
Purpose: Function to generate a cookie to specify country for response
PARAMS: country : string - contains the country you want crawled (look at dictionary at top of file)
        eg. "Singapore"
"""


def generate_country_cookie(country):
    return {"tldp": f"{country_code[country]}"}


"""
Purpose: Function to generate a response for specified URL
PARAMS: url : string - contains URL to a website 
        eg. "https://www.glassdoor.com/Overview/Working-at-eBay-EI_IE7853.11,15.htm"
"""


def get_response(url):
    return httpx.get(
        url,
        cookies=generate_country_cookie("Singapore"),
        follow_redirects=True,
    )


"""
Purpose: Function to scrape glassdoor and get the payload with a specified response
PARAMS: response - contains entire page's CSS which will be read with Selector
"""


def get_glassdoor_payload(company_payload, response):
    selector = Selector(response.text)
    return {
        "employer_name": company_payload["suggestion"],
        "employer_id": company_payload["id"],
        "employer_website": selector.css('[data-test="employer-website"]::text').get(),
        "employer_description": selector.css('[data-test="employerDescription"]::text').get(),
        "employer_size": selector.css('[data-test="employer-size"]::text').get(),
        "employer_revenue": selector.css('[data-test="employer-revenue"]::text').get(),
        "employer_headquarters": selector.css('[data-test="employer-headquarters"]::text').get(),
        "employer_founded": selector.css('[data-test="employer-founded"]::text').get(),
        "employer_industry": selector.css('[data-test="employer-industry"]::text').get()
    }


def find_companies(query: str):
    """find company Glassdoor ID and name by query. e.g. "ebay" will return "eBay" with ID 7853"""
    result = httpx.get(
        url=f"https://www.glassdoor.com/searchsuggest/typeahead?numSuggestions=8&source=GD_V2&version=NEW&rf=full&fallback=token&input={query}",
    )
    data = json.loads(result.content)
    return {
        "suggestion": data[0]["suggestion"],
        "id": data[0]["employerId"]
    }


def payload_to_pandas_dict(payload):
    # value is dict[key]
    for key in payload:
        pandas_dict[key].append(payload[key])


def pandas_dict_to_csv(filename):
    df = pd.DataFrame(pandas_dict)
    df.to_csv(filename)


def extract_apollo_state(html):
    """Extract apollo graphql state data from HTML source"""
    data = re.findall('apolloState":\s*({.+})};', html)[0]
    data = json.loads(data)
    return data


def parse_reviews(html) -> Tuple[List[Dict], int]:
    """parse jobs page for job data and total amount of jobs"""
    cache = extract_apollo_state(html)
    xhr_cache = cache["ROOT_QUERY"]
    reviews = next(v for k, v in xhr_cache.items() if k.startswith("employerReviews") and v.get("reviews"))
    return reviews


async def scrape_reviews(employer: str, employer_id: str, session: httpx.AsyncClient):
    """Scrape job listings"""
    # scrape first page of jobs:
    first_page = await session.get(
        url=f"https://www.glassdoor.com/Reviews/{employer}-Reviews-E{employer_id}_P1.htm",
    )
    reviews = parse_reviews(first_page.text)
    # find total amount of pages and scrape remaining pages concurrently
    total_pages = reviews["numberOfPages"]
    print(f"scraped first page of reviews, scraping remaining {total_pages - 1} pages")
    other_pages = [
        session.get(
            url=str(first_page.url).replace("_P1.htm", f"_P{page}.htm"),
        )
        for page in range(2, total_pages + 1)
    ]
    for page in await asyncio.gather(*other_pages):
        page_reviews = parse_reviews(page.text)
        reviews["reviews"].extend(page_reviews["reviews"])
    return reviews
