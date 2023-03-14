from csv import writer
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import re

#Directory for scrape reviews
scrapeDataPath = "../../dataset/reviews_dataset/"

#Set up header to write into
header = ['Rating', 'Title', 'Author', 'Review Description', 'Pro', 'Con']

# Set the path to the web driver executable for chrome v110
driver_path = '/chromedriver.exe'

#Scraping stuff
def scrape_indeedv2(companies, max_reviews_to_scrape):
    print("##### Starting scrape indeedv2 #####")
    try:
        driver = set_up_driver()
        for company in companies:
            #Get rid of \n
            company = company.strip()
            print("##### Currently scrapping ["+company+"] reviews #####")
            
            #Creating a new .csv for the output of scraped dat
            file = open(scrapeDataPath+company+'_indeed_review.csv', 'w', encoding='utf8', newline='')

            #Open writer to write into file
            fileWriter = writer(file)
            fileWriter.writerow(header)

            #review counter stuff
            review_counter = 0
            reviews_to_scrape = max_reviews_to_scrape

            #start time
            timer = start_timer()

            # Navigate to the website
            url = 'https://sg.indeed.com/cmp/'+company+'/reviews?fcountry=ALL&start'
            driver.get(url)
                
            # Check if there's enough rating to scrape a not else update to number of rating
            total_rating = driver.find_element(By.CSS_SELECTOR,"[data-testid='review-count']").text
            total_rating = re.sub("[^0-9]", "", total_rating)
            total_rating = int(total_rating)
            if(total_rating<reviews_to_scrape):
                reviews_to_scrape=total_rating
            
            print("Number of reviews to scrape: "+str(reviews_to_scrape))

            while review_counter < reviews_to_scrape:
                # Navigate to the website
                url = 'https://sg.indeed.com/cmp/'+company+'/reviews?fcountry=ALL&start='+str(review_counter)
                driver.get(url)
            
                reviews_list = driver.find_element(By.CLASS_NAME,'cmp-ReviewsList')
                reviews = reviews_list.find_elements(By.CLASS_NAME,'css-r0sr81')
                for review in reviews:
                    rating = review.find_element(By.CSS_SELECTOR,"[itemprop='reviewRating']").text
                    title = review.find_element(By.CSS_SELECTOR,"[data-testid='title'").text
                    author = review.find_element(By.CSS_SELECTOR,"[itemprop='author']").text
                    review_description = review.find_element(By.CSS_SELECTOR,"[data-tn-component='reviewDescription']").text
                    pro_con = review.find_elements(By.CLASS_NAME,"css-1z0411s")
                    pro = ""
                    con = ""

                    #Check if have pros/cons reviews
                    if(len(pro_con)>1):
                        pro = pro_con[0].text
                        con = pro_con[1].text

                    info = [rating, title, author, review_description, pro, con]

                    # The writer will then write this info the the CSV
                    fileWriter.writerow(info)
                
                review_counter+=20
                print("----------------------------------------------------")
                print(str(review_counter)+" reviews has been scrape for " + company)
                estimate_time(timer, review_counter, reviews_to_scrape)
                

            #Close files
            file.close()
            print("###### ["+company+"] done scraping#####")
        
        #Quit driver
        driver.quit()
        print("\nFinish scrapping all companies")
        
    except Exception as e:
        print("ERROR: "+str(e))


# Create a new instance of the web driver
def set_up_driver():
    print("Setting up driver")
    return webdriver.Chrome(executable_path=driver_path)

# Start timer
def start_timer():
    return time.time()

# Use to estimate timer
def estimate_time(start_time, review_counter, reviews_to_scrape):
    end_time = time.time()
    elapsed_time = end_time - start_time
    review_left = reviews_to_scrape - review_counter
    estimated_time_left = elapsed_time/review_counter*review_left
    print(f"Estimated time left: {estimated_time_left:.2f} seconds\tReviews left: {review_left}")