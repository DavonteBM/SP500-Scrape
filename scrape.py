#Imports
from bs4 import BeautifulSoup
import requests
import psycopg2
import numpy as np
import pandas as pd

#Connect and create Database called sp500
conn = psycopg2.connect(database="postgres", user='postgres', password=' ', host = 'localhost', 
port='5432'
)
cur = conn.cursor()
cur.execute(""" CREATE TABLE IF NOT EXISTS sp500(symbol TEXT, name TEXT, gcis_sector TEXT, sub_industry TEXT, hq TEXT, date_added TEXT, cik TEXT, founded TEXT) """)

#Parse the list of sp500 companies page 
url ="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
page = requests.get(url)
soup = BeautifulSoup(page.content, 'html.parser')

#Find the elements to be scraped
table = soup.find('tbody', )
table2 = table.findAll('td')

#Scraping and then cleaning the data from the sp500 table into a list called scrapedData
scrapedData =[]
for item in table2:
    result = item.getText()
    cleanedData = result.strip()
    scrapedData.append(cleanedData)

#Creating and reshaping an array from the scraped Data and turning it into a dataframe
array = np.array(scrapedData)
processedArray = array.reshape(503, 8)
df = pd.DataFrame(processedArray)

#Create SQL insert command to insert dataframe into the table
query = """INSERT INTO sp500(symbol, name, gcis_sector, sub_industry, hq, date_added, cik, founded) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""" 

#Loop through the number of items in the dataframe and inserting the item into the database
for x in range(len(df)):
 cur.execute(query, df.iloc[x])

#Commit changes to database
conn.commit()

#Show that web scrape into the database was successful by getting all items from the table
cur.execute("""SELECT * FROM sp500""")
sp500 =cur.fetchall()
for i in sp500:
  print(i)

#End connection of the database and the cursor object
cur.close()
conn.close()