import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3

def fetch_driver_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        return soup
    else:
        return None
    

def race_urls(year):
    url = 'https://www.formula1.com/en/results.html/'+str(year)+'/races.html'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    race_links = []
    
    for link in soup.find_all("a", class_="resultsarchive-filter-item-link"):
        href = link['href']
        if '/'+str(year)+'/races/' in href and '/race-result.html' in href:
            race_url = 'https://www.formula1.com' + href
            race_links.append(race_url)
    
    return race_links


def store_driver_data(driver_data, file_path='driver_standings.csv'):
    df = pd.DataFrame(driver_data)
    df.to_csv(file_path, index=False)
    
def parse_driver_data(soup):
    
    if soup.find('table', class_='resultsarchive-table') != None:
        driver_table = soup.find('table', class_='resultsarchive-table')
        rows = driver_table.find_all('tr')
        driver_data = []

        for row in rows[1:]:  # Skip the header row
            cells = row.find_all('td')
            driver_info = {
                'position': (cells[1].text.strip()),
                'name': cells[3].text.strip().replace('\n', ' '),
                'team': cells[4].text.strip(),
                'points': (cells[7].text.strip()),
            }
            driver_data.append(driver_info)
        return driver_data
    return None


def upload_to_s3(file_name, object_name, bucket_name):
    s3 = boto3.client('s3')
    s3.upload_file(file_name, bucket_name, object_name)

def download_from_s3(object_name, file_name, bucket_name):
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, object_name, file_name)


def main():
    
    file_name = 'driver_standings.csv'
    object_name = 'driver_standings.csv'
    bucket_name = 'formula-one-driver-data'

    # Upload the CSV file to S3
    upload_to_s3(file_name, object_name, bucket_name)

    # Download the CSV file from S3
    download_from_s3(object_name, file_name, bucket_name)


    bucket_name = 'my-bucket-name'
    file_name = 'driverdata.csv'
    object_name = 'driverdata.csv'

    totalData = []
    race_locations = race_urls(2023)
    for race in race_locations:
        soup = fetch_driver_data(race)
        if soup:
            driver_data = parse_driver_data(soup)
            if driver_data is not None:
                totalData = totalData + driver_data
    
    store_driver_data(totalData)
if __name__ == '__main__':
    main()
