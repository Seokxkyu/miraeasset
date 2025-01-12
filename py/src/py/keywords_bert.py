import pandas as pd
import re
from konlpy.tag import Okt
from keybert import KeyBERT

class Preprocessor:
    def __init__(self):
        self.okt = Okt()
        self.stopwords = [
            '은', '는', '이', '가', '의', '에', '을', '를', '도', '으로', '에서', '한', '하다', '지만', '우선', '먼저',
            '수량', '가격', '기록', '기자', '지난해', '이번', '최근', '거래', '주가', '앵커', '멘트', '기상캐스터',
            '뉴스', '지원', '연구원', '국내', '회장', '대표', '계약', '응모', '영향', '지수', '시장', '성장', '활동',
            '상위', '만원', '억원', '억억원', '지난', '이후', '이전', '점유율', '자산', '이기', '때문', '때문에',
            '수익', '매출', '하락', '상승', '데이터', '분석', '발표', '등록', '의견', '결과', '상황'
        ]
    
    def clean(self, text):
        text = re.sub('([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)', '', text)
        text = re.sub('(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', '', text)
        text = re.sub('([ㄱ-ㅎㅏ-ㅣ]+)', '', text)
        text = re.sub('([a-zA-Z0-9]+)', '', text)
        text = re.sub('<[^>]*>', '', text)
        text = re.sub('[^\w\s]', '', text)
        return text

    def nouns(self, text):
        nouns = self.okt.nouns(text)
        nouns = [noun for noun in nouns if noun not in self.stopwords]  # 불용어 제거
        return ' '.join(nouns)

    def process(self, text):
        text = self.clean(text)
        text = self.nouns(text)
        return text

class KeyBERTExtractor:
    def __init__(self):
        self.preprocessor = Preprocessor()
        self.model = KeyBERT()

    def extract(self, df, content_column='content', top_n=20):
        articles = df[content_column].tolist()
        articles = [article if isinstance(article, str) else '' for article in articles]
        
        processed_articles = [self.preprocessor.process(article) for article in articles]

        keywords = {}
        for idx, article in enumerate(processed_articles):
            top_keywords = self.model.extract_keywords(article, keyphrase_ngram_range=(1, 1), stop_words='english', top_n=top_n)
            keywords[f'Article {idx+1}'] = ', '.join([kw[0] for kw in top_keywords])

        df['keywords'] = [keywords[f'Article {i+1}'] for i in range(len(articles))]
        return df

def extract_keybert(ds_nodash, output_dir):
    input_parquet = f"{output_dir}/content/{ds_nodash}.parquet"
    output_parquet = f"{output_dir}/bert/{ds_nodash}.parquet"

    df = pd.read_parquet(input_parquet)

    extractor = KeyBERTExtractor()

    df = extractor.extract(df, content_column='content')
    df.to_parquet(output_parquet, index=False)
    return df
