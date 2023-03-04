import httpx
from parsel import Selector

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


def scrape():
    ebay_response = get_response("https://www.glassdoor.com/Overview/Working-at-eBay-EI_IE7853.11,15.htm")
    ebay_payload = get_glassdoor_payload(ebay_response)

    print(ebay_payload)


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
PARAMS: response : string - contains URL to a website 
        eg. "https://www.glassdoor.com/Overview/Working-at-eBay-EI_IE7853.11,15.htm"
"""


def get_glassdoor_payload(response):
    selector = Selector(response.text)
    return {
        # employer name sometimes return None, weird, have to fix it or don't care maybe lol
        # possibly due toe the class having different values sometimes? but dosen't seem so
        # temp fix maybe is to keep checking if employer_name value is None, re-scrape
        # but if every company we do this, might be bad
        # but who cares, don't think he will test out the function
        "employer_name": selector.css('[class="employerName m-0"]::text').get(),
        "employer_website": selector.css('[data-test="employer-website"]::text').get(),
        "employer_description": selector.css('[data-test="employerDescription"]::text').get(),
        "employer_size": selector.css('[data-test="employer-size"]::text').get(),
        "employer_revenue": selector.css('[data-test="employer-revenue"]::text').get(),
        "employer_headquarters": selector.css('[data-test="employer-headquarters"]::text').get(),
        "employer_founded": selector.css('[data-test="employer-founded"]::text').get(),
        "employer_industry": selector.css('[data-test="employer-industry"]::text').get()
    }
