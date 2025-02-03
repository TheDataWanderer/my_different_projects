import pandas as pd
import requests
from bs4 import BeautifulSoup
import csv

def parser(url:str):
    response = requests.get(url=url)
    soup = BeautifulSoup(response.text, 'lxml')
    products = soup.find_all('tbody')
    print(products)
def create_csv():
    pass

def write_csv():
    pass

if __name__ == '__main__':
    parser(url='https://ru.wikipedia.org/wiki/%D0%A4%D0%B5%D0%B4%D0%B5%D1%80%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D0%B5_%D0%BE%D0%BA%D1%80%D1%83%D0%B3%D0%B0_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B9%D1%81%D0%BA%D0%BE%D0%B9_%D0%A4%D0%B5%D0%B4%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D0%B8')