# miraeasset_news
`news_data`는 주간마다 실행되는 Apache Airflow DAG으로, 뉴스 데이터를 수집, 요약, 키워드 추출, 그리고 결과 저장 등의 작업을 자동화합니다.

## DAG
![image](https://github.com/user-attachments/assets/b244c62b-029b-46f7-86b0-6313afb607e1)

### 1. crawl_task
Description: 특정 날짜의 네이버 금융 뉴스 기사를 크롤링하여 기사 URL, 제목, 본문 내용을 수집하고, 이를 parquet 파일로 저장<br>
python callable: `crawl_data`<br>
입력: 실행 날짜 `{{ ds_nodash }}`<br>
출력: `output_dir/content/{{ ds_nodash }}.parquet`

### 2. keywords_keybert
Description: 뉴스 본문에서 KeyBERT 기반으로 핵심 키워드 추출하여 저장<br>
python callable: `extract_keybert`<br>
입력: `output_dir/content/{{ ds_nodash }}.parquet`<br>
출력: `output_dir/bert/{{ ds_nodash }}.parquet`

### 3. keywords_tfidf
Description: TF-IDF 기반으로 늇, 본문에서 주요 키워드를 추출하여 저장<br>
python callable: `extract_keywords`<br>
입력: `output_dir/bert/{{ ds_nodash }}.parquet`<br>
출력: `output_dir/keywords/{{ ds_nodash }}.parquet`

### 4. summarize_task
Description: 뉴스 본문 내용 요약하여 저장<br>
python callable: `summarize_articles`<br>
입력: `output_dir/keywords/{{ ds_nodash }}.parquet`<br>
출력: `output_dir/summarize/{{ ds_nodash }}.parquet`

### 5. keywords_api
Description: 네이버 클로바 API를 사용하여 뉴스 본문에서 키워드를 추출하고 저장<br>
python callable: `keyword_naver`<br>
입력: `output_dir/summarize/{{ ds_nodash }}.parquet`<br>
출력: `output_dir/keywords_api/{{ ds_nodash }}.parquet`
