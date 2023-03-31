import pandas as pd
import string
import os

folder_input = "../../dataset/reviews_dataset/"
folder_output = "../../dataset/clean_dataset_v2/"

# folder_input = "../../dataset/test/"
# folder_output = "../../dataset/test/"

for filename in os.listdir(folder_input):
    filepath = os.path.join(folder_input, filename)
    if os.path.isfile(filepath):
        print(filepath)
        if("meta_glassdoor" in filepath):
            df = pd.read_csv(filepath, encoding='ISO-8859-1') # add encoding='ISO-8859-1' in function for those that cant work
        else:
            df = pd.read_csv(filepath) # add encoding='ISO-8859-1' in function for those that cant work

        for index, row in df.iterrows():
            for column in df.columns:
                # Remove spaces and punctuation from the string
                cleaned_string = str(row[column]).translate(str.maketrans('', '', string.punctuation))
                cleaned_string = ' '.join(cleaned_string.split())

                #Fix the rating number
                if(column == "Rating"):
                    if(float(cleaned_string)>9):                        
                        row[column] = int(int(cleaned_string)/10)
                    elif(cleaned_string.isdigit()):
                        row[column] = int(cleaned_string)
                    
                else:
                    row[column] = cleaned_string
            df.loc[index] = row
        if ("glassdoor" in filename):
            df = df.drop(df.columns[0], axis=1)
            df.insert(loc=2, column='Review Description', value='nan')

        #Convert date to be consistent
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%d/%m/%Y')
        #Save rating as a int, if nan set as 0
        df['Rating'] = df['Rating'].fillna(0).astype(int)

        #Update file name
        df.to_csv(folder_output+filename.lower().replace('.csv', '_clean.csv'), index=False)
        print(df)
