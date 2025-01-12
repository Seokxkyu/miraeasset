import pandas as pd
import re
from konlpy.tag import Okt
from sklearn.feature_extraction.text import TfidfVectorizer

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

class TFIDFExtractor:
    def __init__(self):
        self.preprocessor = Preprocessor()
        self.vectorizer = TfidfVectorizer()

    def extract(self, df, content_column='content', top_n=20):
        articles = df[content_column].tolist()
        articles = [article if isinstance(article, str) else '' for article in articles]
        
        processed_articles = [self.preprocessor.process(article) for article in articles]

        tfidf_matrix = self.vectorizer.fit_transform(processed_articles)
        tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=self.vectorizer.get_feature_names_out())

        keywords = {}
        for idx, row in tfidf_df.iterrows():
            top_keywords = row.sort_values(ascending=False).head(top_n).index
            keywords[f'Article {idx+1}'] = ', '.join(top_keywords.tolist())

        df['keywords_tfidf'] = [keywords[f'Article {i+1}'] for i in range(len(articles))]
        return df

def extract_keywords(ds_nodash, output_dir):
    input_parquet = f"{output_dir}/bert/{ds_nodash}.parquet"
    output_parquet = f"{output_dir}/keywords/{ds_nodash}.parquet"

    df = pd.read_parquet(input_parquet)

    extractor = TFIDFExtractor()

    df = extractor.extract(df, content_column='content')
    df['date'] = ds_nodash
    df.to_parquet(output_parquet, index=False)
    return df
