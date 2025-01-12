import os
import pandas as pd
import http.client
import json
import time

class CompletionExecutor:
    def __init__(self):
        self._host = 'clovastudio.apigw.ntruss.com'
        self._api_key = os.getenv('CLOVA_API_KEY')
        self._api_key_primary_val = os.getenv('CLOVA_API_PRIMARY_KEY')
        self._request_id = '9adb0137-8663-4035-b40c-9c16866f0c9e'
        self._seg_min_size = 500  # 최소 요약 단위 크기
        self._seg_max_size = 2000  # 최대 요약 단위 크기
        self._seg_count = -1  # 모델이 최적 문단 수로 분리하도록 설정
        self._auto_sentence_splitter = True  # 문장 분리 허용 여부
        self._include_ai_filters = True  # AI 필터 적용 여부

    def _send_request(self, completion_request):
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'X-NCP-CLOVASTUDIO-API-KEY': self._api_key,
            'X-NCP-APIGW-API-KEY': self._api_key_primary_val,
            'X-NCP-CLOVASTUDIO-REQUEST-ID': self._request_id
        }

        conn = http.client.HTTPSConnection(self._host)
        conn.request('POST', '/testapp/v1/api-tools/summarization/v2/fea095400a684ef3a9b6e4f03959c1b0', json.dumps(completion_request), headers)
        response = conn.getresponse()
        result = json.loads(response.read().decode(encoding='utf-8'))
        conn.close()
        return result

    def execute(self, text):
        request_data = {
            "texts": [text],
            "segMinSize": self._seg_min_size,
            "segMaxSize": self._seg_max_size,
            "segCount": self._seg_count,
            "autoSentenceSplitter": self._auto_sentence_splitter,
            "includeAiFilters": self._include_ai_filters
        }

        time.sleep(2)
        res = self._send_request(request_data)
        if res['status']['code'] == '20000':
            return res['result']['text']
        else:
            return 'Error: ' + res['status']['message']

def summarize_articles(ds_nodash, output_dir):
    input_parquet = f"{output_dir}/keywords/{ds_nodash}.parquet"
    output_parquet = f"{output_dir}/summarize/{ds_nodash}.parquet"
    
    df = pd.read_parquet(input_parquet)
    completion_executor = CompletionExecutor()

    df['summarize'] = df['content'].apply(lambda x: completion_executor.execute(x))
    time.sleep(10)
    df.to_parquet(output_parquet, index=False)

    return df
