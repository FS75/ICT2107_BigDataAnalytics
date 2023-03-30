import pandas as pd
import string

fileInput = "../../dataset/reviews_dataset/Amazon_glassdoor_review.csv"
fileOutput = "../../dataset/clean_dataset/Amazon_glassdoor_review_cleaned.csv"

df = pd.read_csv(fileInput)

for index, row in df.iterrows():
    for column in df.columns:
        # Remove spaces and punctuation from the string
        cleaned_string = str(row[column]).translate(str.maketrans('', '', string.punctuation))
        cleaned_string = ' '.join(cleaned_string.split())
        row[column] = cleaned_string
    df.loc[index] = row

df.to_csv(fileOutput, index=False)
print(df)
