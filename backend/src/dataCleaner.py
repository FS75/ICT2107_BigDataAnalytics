import pandas as pd
import string
import os

folder_input = "../../dataset/reviews_dataset/"
folder_output = "../../dataset/clean_dataset/"

for filename in os.listdir(folder_input):
    # if "glassdoor" in filename:
    # you can use this @Juleus but remember to indent the rest of the code below
    
    filepath = os.path.join(folder_input, filename)
    if os.path.isfile(filepath):
        print(filepath)

        df = pd.read_csv(filepath)

        for index, row in df.iterrows():
            for column in df.columns:
                # Remove spaces and punctuation from the string
                cleaned_string = str(row[column]).translate(str.maketrans('', '', string.punctuation))
                cleaned_string = ' '.join(cleaned_string.split())
                row[column] = cleaned_string
            df.loc[index] = row

        df.to_csv(folder_output+filename.replace('.csv', '_clean.csv'), index=False)
        print(df)
