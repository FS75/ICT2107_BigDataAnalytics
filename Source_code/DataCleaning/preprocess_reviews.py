import csv
import re
import os
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')
nltk.download('punkt')

input_csv_file = ["apple_glassdoor_review_clean.csv","apple_indeed_review_clean.csv","dbs-bank_glassdoor_review_clean.csv",
                  "dbs-bank_indeed_review_clean.csv","google_glassdoor_review_clean.csv","google_indeed_review_clean.csv",
                  "hsbc_glassdoor_review_clean.csv","hsbc_indeed_review_clean.csv","infineon-technologies_glassdoor_review_clean.csv",
                  "infineon-technologies_indeed_review_clean.csv","meta_glassdoor_review_clean.csv","meta_indeed_review_clean.csv","micron-technology_glassdoor_review_clean.csv",
                  "micron-technology_indeed_review_clean.csv","netflix_glassdoor_review_clean.csv","netflix_indeed_review_clean.csv","ntuc-fairprice_glassdoor_review_clean.csv",
                  "ntuc-fairprice_indeed_review_clean.csv","sembcorp_glassdoor_review_clean.csv","sembcorp_indeed_review_clean.csv","st-engineering_glassdoor_review_clean.csv",
                  "st-engineering_indeed_review_clean.csv","united-overseas-bank_glassdoor_review_clean.csv","united-overseas-bank_indeed_review_clean.csv"]


stop_words = set(stopwords.words('english'))

def clean_text(text):
    if text is None or text.lower() == 'nan':
        return ''
    
    text = re.sub(r'[^a-zA-Z\s]', ' ', text)  # Remove special characters and punctuation
    text = text.lower()  # Convert to lowercase
    tokens = nltk.word_tokenize(text)  # Tokenize the text
    tokens = [token for token in tokens if token not in stop_words]  # Remove stopwords
    text = ' '.join(tokens)  # Join tokens back into a single string
    return text



i = 1
for review in input_csv_file:
    output_text_file = review.split(".")[0] + '_preprocessed_reviews.txt'
    with open(review, 'r', encoding='utf-8') as csv_file, open(output_text_file, 'w', encoding='utf-8') as text_file:
        csv_reader = csv.DictReader(csv_file)
        
        for row in csv_reader:
            pro = clean_text(row['Pro'])
            con = clean_text(row['Con'])
            description = clean_text(row['Review Description'])
            combined_review = f'{pro} {con} {description}'.strip()
            
            if combined_review:
                text_file.write(f'{combined_review}\n')



    print(f'Preprocessed reviews saved to {output_text_file}\n\n')
    i+=1