import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# Convert URL function
def convert_url(url):
    parts = url.split('article_id=')
    if len(parts) > 1:
        article_id = parts[1].split('&')[0]
        office_id = url.split('office_id=')[1].split('&')[0]
        return f'https://n.news.naver.com/mnews/article/{office_id}/{article_id}'
    return url

# Scrape article content
def scrape_content(url):
    url = convert_url(url)
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    title_tag = soup.find('h2', {'id': 'title_area'}) or \
                soup.find('h3', {'id': 'articleTitle'})
    title = title_tag.get_text(strip=True) if title_tag else 'no title'

    content_tag = soup.find('div', {'id': 'dic_area'}) or \
                  soup.find('div', {'id': 'newsct_article'}) or \
                  soup.find('div', {'id': 'articleBodyContents'})
    content = content_tag.get_text(separator=" ", strip=True) if content_tag else 'no content'

    return title, content

# Get article URLs
def fetch_urls(date, max_pages=5):
    base_url = f"https://finance.naver.com/news/mainnews.naver?date={date}"
    urls = []
    for page in range(1, max_pages + 1):
        search_url = f"{base_url}&page={page}"
        response = requests.get(search_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        items = soup.find_all('dd', class_='articleSubject')
        if not items:
            break
        for item in items:
            url = 'https://finance.naver.com' + item.find('a')['href']
            urls.append(url)
    return urls

# Crawl data for a specific date
def crawl_data(date, output_dir):
    urls = fetch_urls(date)
    data = []
    for url in urls:
        title, content = scrape_content(url)
        data.append([url, title, content])

    df = pd.DataFrame(data, columns=['URL', 'title', 'content'])
    
    file_name = f"{output_dir}/content/{date}.parquet"
    df.to_parquet(file_name, index=False, engine='pyarrow')
    # file_name = f"{output_dir}/{date}.csv"
    # df.to_csv(file_name, index=False, encoding='utf-8-sig')
