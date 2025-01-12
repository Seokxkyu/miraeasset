import pandas as pd
import requests
import time
import os

class ClovaKeywordExtractor:
    def __init__(self):
        self._host = 'https://clovastudio.stream.ntruss.com'
        self._api_key = os.getenv('CLOVA_API_KEY')
        self._api_key_primary_val = os.getenv('CLOVA_API_PRIMARY_KEY')
        self._request_id = 'bfe0587d-b8b3-4ad5-ae87-62f35b0dd233'

    def _send_request(self, request_data):
        headers = {
            'X-NCP-CLOVASTUDIO-API-KEY': self._api_key,
            'X-NCP-APIGW-API-KEY': self._api_key_primary_val,
            'X-NCP-CLOVASTUDIO-REQUEST-ID': self._request_id,
            'Content-Type': 'application/json; charset=utf-8',
        }

        response = requests.post(self._host + '/testapp/v1/chat-completions/HCX-003',
                                 headers=headers, json=request_data)
        
        time.sleep(2)

        if response.status_code == 200:
            result = response.json()
            if 'result' in result and 'message' in result['result']:
                return result['result']['message']['content']
            else:
                return "Error: 'message' key not found in the result"
        else:
            return f"Error: {response.status_code}, {response.text}"

    def keyword_naver(self, text):
        system_message = """-문서에서 핵심 키워드를 추출합니다.\n-키워드는 핵심 주제와 상응하는 우선순위로 추출합니다.\n\n###\n최근 국내 경제는 코로나19 확산세가 지속되면서 소비와 투자가 위축되는 모습을 보이고 있습니다. 수출은 반도체 등 주력 품목의 호조로 증가세를 유지하고 있지만, 글로벌 공급망 차질과 원자재 가격 상승 등의 영향으로 불확실성이 커지고 있습니다. 주식시장은 미국의 금리 인상과 인플레이션 우려 등으로 인해 변동성이 확대되고 있습니다. 채권시장은 금리 상승 압력이 높아지면서 수익률이 하락하고 있으며, 외환시장은 원화 강세가 지속되고 있습니다. 국내 대표 기업들은 대체로 양호한 실적을 발표하였습니다. 하지만, 일부 기업들은 코로나19 장기화와 글로벌 경기 침체 등으로 인해 실적이 악화되는 모습을 보였습니다.한국은행은 기준금리를 동결하였으며, 금융당국은 대출 규제와 가계부채 관리 강화 등의 정책을 추진하고 있습니다. 미국은 인플레이션 우려가 지속되면서 금리 인상 속도가 빨라지고 있습니다. 중국은 XX그룹 파산 위기 등으로 인해 경제성장률이 둔화될 것으로 예상됩니다. 일본은 코로나19 재확산으로 인해 경제활동이 위축되고 있습니다.\n키워드 : \n#국내 경제 동향\n- 코로나19 확산세\n- 소비 위축\n- 투자 위축\n- 수출 증가\n- 글로벌 공급망 차질\n- 원자재 가격 상승\n\n금융시장 동향\n- 주식시장 변동성 확대\n- 채권시장 수익률 하락\n- 외환시장 원화 강세\n\n#주요 기업 실적\n- 코로나19 장기화\n- 글로벌 경기 침체\n\n#금융정책 동향\n- 한국은행 기준금리 동결\n- 대출 규제\n- 가계부채 관리 강화\n\n#해외 경제 동향\n- 미국 인플레이션 우려\n- 금리 인상 속도\n- 중국 XX그룹 파산 위기\n- 일본 코로나19 재확산"""

        preset_text = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": text}
        ]

        request_data = {
            'messages': preset_text,
            'topP': 0.6,
            'topK': 0,
            'maxTokens': 300,
            'temperature': 0.2,
            'repeatPenalty': 2.0,
            'stopBefore': [],
            'includeAiFilters': True,
            'seed': 0
        }

        return self._send_request(request_data)

class NewsKeywordExtractor:
    def __init__(self):
        self.keyword_extractor = ClovaKeywordExtractor()

    def extract(self, df, content_column='content'):
        df['keywords_naver'] = df[content_column].apply(
            lambda text: self.keyword_extractor.keyword_naver(text)
        )
        return df

def keyword_naver(ds_nodash, output_dir):
    input_parquet = f"{output_dir}/summarize/{ds_nodash}.parquet"
    output_parquet = f"{output_dir}/keywords_api/{ds_nodash}.parquet"

    df = pd.read_parquet(input_parquet)

    extractor = NewsKeywordExtractor()

    df = extractor.extract(df, content_column='content')
    df.to_parquet(output_parquet, index=False)
    return df
