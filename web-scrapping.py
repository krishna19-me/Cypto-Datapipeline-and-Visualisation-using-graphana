import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import ast
from datetime import datetime
import psycopg2
import psycopg2.extras as extras
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

# Flask constructor takes the name of
# current module (_name_) as argument.
app = Flask(__name__)

url = 'https://finance.yahoo.com/crypto/'
ENDPOINT= 'rds-database-1.ctbfi1ghnc8t.ap-south-1.rds.amazonaws.com'
PORT="5432"
USER="postgres"
REGION="ap-south-1a"
DBNAME="crypto_data"

def getCryptoData(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find the HTML element that contains the data you want to extract
    data_table = soup.find('table', {'class': 'W(100%)'})

    # Extract the data from the table using BeautifulSoup
    rows = data_table.find_all('tr')
    crypto_data = []
    for row in rows:
        cells = row.find_all('td')
        if len(cells) > 0:
            symbol = cells[0].text
            name = cells[1].text
            price = cells[2].text
            change = cells[3].text
            market_cap = cells[4].text
            volume = cells[5].text
            time = datetime.now()
            price = price.replace(',','')
            # Create a dictionary to store the extracted data
            crypto_dict = {'symbol': symbol,'name':name, 'price': float(price), 'change': change, 'market_cap': market_cap, 'volume': volume, 'updated_time': str(time)}
            crypto_data.append(crypto_dict)
    return crypto_data

def execute_values(conn, df, table):
    # print(df.to_numpy())
    tuples = [tuple(x) for x in df.to_numpy()]  
    cols = ','.join(list(df.columns))
    # print(tuples)
  
    # SQL query to execute
    # delete ="Delete from "+table
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    # print(query)
    cursor = conn.cursor()
    try:
        # cursor.execute(delete)
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("Queries Executed Successfully!!!")
    cursor.close()


def runPipeline():
    crypto_data = getCryptoData(url=url)
    # Serialize the data to a JSON string
    # print(crypto_data)
    json_data = json.dumps(crypto_data)
    dfdata = ast.literal_eval(json_data)
    df = pd.DataFrame(dfdata)
    conn = psycopg2.connect(database=DBNAME,user='postgres',password='postgres',host=ENDPOINT,port=PORT)
    execute_values(conn,df,'CRYPTO_DATA')
    print(df)

@app.route("/")
def scrape_run():
    print("Running Web Scrapper Application!")

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=runPipeline, trigger='cron', minute='*/1')
    scheduler.start()
    app.run()
