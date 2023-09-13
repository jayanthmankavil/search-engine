import os
import re
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
from urllib.parse import urlparse
import sqlite3
import csv

index_data = defaultdict(list)

metadata = {}

def extract_metadata(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string.strip() if soup.title else ''
        description = soup.find('meta', attrs={'name': 'description'})
        description = description['content'].strip() if description else ''
        metadata[url] = {'title': title, 'description': description}
    except Exception as e:
        print(f"Error extracting metadata for {url}: {e}")

def crawl_and_index(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        text = ' '.join(soup.stripped_strings)
        words = re.findall(r'\w+', text.lower())

        for word in words:
            index_data[word].append(url)

        extract_metadata(url)

    except Exception as e:
        print(f"Error crawling {url}: {e}")

def read_urls_from_csv(filename):
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return [row['url'] for row in reader]

csv_filename = 'urls.csv'

sample_urls = read_urls_from_csv(csv_filename)

conn = sqlite3.connect('crawler.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS indexed_data (
        word TEXT,
        url TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS metadata (
        url TEXT PRIMARY KEY,
        title TEXT,
        description TEXT
    )
''')

for url in sample_urls:
    crawl_and_index(url)

    for word in index_data.keys():
        cursor.executemany("INSERT INTO indexed_data (word, url) VALUES (?, ?)", [(word, u) for u in index_data[word]])

    for url, meta in metadata.items():
        cursor.execute("INSERT OR REPLACE INTO metadata (url, title, description) VALUES (?, ?, ?)",
                       (url, meta.get('title', ''), meta.get('description', '')))

conn.commit()
conn.close()
