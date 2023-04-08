# ICT2107_BigDataAnalytics
A big data analytics project done for the module ICT 2107 (Distributed Systems Programming)

This project aims to showcase the use of big data analytics in solving real-world problems. The project was done as a part of the ICT 2107 module, which focuses on Distributed Systems Programming.

## Getting started
To get started with this project, you may refer to [running instruction](https://github.com/FS75/ICT2107_BigDataAnalytics/blob/main/Group02/Running_Instruction.pdf). To take a look at our visualization you may proceed [here](https://github.com/FS75/ICT2107_BigDataAnalytics/tree/main/Group02/Source_code/DataVisualization). The pbix files contain a PowerBI file that requires the use of PowerBI to open while the pdf containing a brief look at how the visualization looks.
![image](https://user-images.githubusercontent.com/24997286/230725677-4ab06832-cca7-4fca-b10a-94fe7e825085.png)


Below this section we also describe the file structure for a better understanding.

## File Structure

This project consists of the following folders:
  - `Dataset_used`: Folder containing datasets that we scraped, cleaned, processed and used for our analysis
  - `JAR`: Folder containing JAR that were used to run anaylsis on dataset in Hadoop
  - `Report`: Folder containing a report written in IEEE format that we submitted to our school
  - `Source_code`: Folder containing all the codes that was used for scrapping, cleaning, analysis and visualization
  - `Running_Instruction.pdf`: A document specifying how to run the codes for this project

### `Dataset_used` folder
  - `AnalysisOutput`: Folder containing all the analysis output that were used in PowerBI for visualization
  - `CleanDataset`: Folder containing ReviewsDataset that has been clean using [DataCleaning/dataCleaner.py](https://github.com/FS75/ICT2107_BigDataAnalytics/tree/main/Group02/Source_code/DataCleaning)
  - `ProcessedDataset`: Folder containing CleanDataset which has been processed using [DataCleaning/preprocess_reviews.py](https://github.com/FS75/ICT2107_BigDataAnalytics/tree/main/Group02/Source_code/DataCleaning)
  - `ReviewsDataset`: Folder containing scrape reviews dataset from various company obtained through [DataScraping/](https://github.com/FS75/ICT2107_BigDataAnalytics/tree/main/Group02/Source_code/DataScraping) 
  - `AFINN-111.txt`: A text file containing sentiment values tagged to each words
  - `company-industry.txt`: A text file containing the company-to-industry relationship
  - `stopwords.txt`: A text file containing stop words to skip for analysis

### `Source_code` folder
  - `Analysis`: Folder containing codes written in Java used for analysis
  - `DataCleaning`: Folder containing python code that was used to clean or process reviews
  - `DataScraping`: Folder containg python code that was used to scrape datasets
  - `DataVisualization`: PowerBI report that was used to generate visualization


## Contributors
| Name | Contribution |
| -------- | -------- |
| Bruce Wang | Data Scraping (Indeed), Automation of Data Cleaning & Data Visualization |
| Juleus Seah | Data Scraping (Glassdoor) & Data Visualization |
| Lim Ryan | Data Cleaning & Word Count Analysis |
| Kang Chen | Stopwords Cleaning, Sentiment Analysis & Industry Trend Analysis |
| Liu Jun | Industry Trend Analysis |
| Chun Boon | Data Processing & Topic Modelling |
