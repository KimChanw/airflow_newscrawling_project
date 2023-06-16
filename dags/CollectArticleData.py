import requests
import logging
import pandas as pd
import re
import time

from bs4 import BeautifulSoup
from typing import List, Tuple
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

@task
def get_latest_article_link() -> str:
    """기사 메인 사이트에서 최신 기사 링크 크롤링하는 함수"""
    url = f'https://world.kbs.co.kr/service/news_list.htm?lang=k'
    
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    # 최신 기사가 있는 css selector에서 기사 링크를 가져옴
    selector = '#container > div > section.comp_contents_1x > article:nth-child(1) > h2 > a'
    select_res = soup.select(selector)

    # 기사 링크가 있는 href 속성
    # . 으로 시작하기 때문에 그 이후부터 추출
    news_link = select_res[0]['href'][1:]
    
    # 메인 사이트 url이랑 합쳐 최신 기사 링크 반환
    return news_link

@task
def extract_news_num(news_link) -> int:
    """정규식을 이용하여 기사 url에서 기사 번호를 추출하는 함수"""
    
    # 숫자를 추출하는 정규식 패턴    
    num_pattern = re.compile('[0-9]+')

    # 기사 url에서 뉴스 기사 추출 -> 리스트로 반환 
    # 예시 url : https://news.kbs.co.kr/news/view.do?ncd=7701599
    news_num = num_pattern.findall(news_link)
    
    # 177309만 추출, 정수형으로 변환
    return int(news_num[0])

@task
def extract_latest_articles(latest_news_idx, news_cnt=5) -> dict:
    """
    기사의 제목과 기사 내용을 추출하는 함수
    parameter:
    latest_news_idx : 최신 기사 번호
    news_cnt : 추출할 기사 개수
    """
    news_titles = []
    news_contents = []
    times = []
    article_nums = []
    
    # 최신 기사 번호에서 1씩 감소해가며 크롤링
    for i in range(news_cnt):
        news_idx = latest_news_idx - i
        
        url = f'https://news.kbs.co.kr/news/view.do?ncd={news_idx}'
        
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 태그와 클래스 이름을 통해 기사 제목과 내용을 찾음
        title = soup.find('h5', 
                          {'class' : 'tit-s'}).get_text()
        
        content = soup.find('div',
                            {'class' : 'detail-body font-size'}).get_text()
        
        write_time = soup.find('em',
                               {'class' : 'date'}).get_text()
        
        news_titles.append(title)
        news_contents.append(content)
        times.append(write_time)
        article_nums.append(news_idx)
        
        time.sleep(0.5)
        
    extract_result_dict = {
        'news_title' : news_titles,
        'news_contents' : news_contents,
        'write_time' : times,
        'article_nums' : article_nums
    }
    
    # 딕셔너리의 모든 value 결과는 리스트의 모음이어야 함
    logging.info(type(list(extract_result_dict.values())))    
    logging.info(extract_result_dict)
    
    return extract_result_dict

@task
def preprocess_data(**kwargs) -> List[dict]:
    """정규식을 이용하여 기사 제목과 컨텐츠에서 불필요한 문자를 제거하는 함수"""
    titles, contents, times, article_nums = kwargs['titles'], kwargs['contents'], kwargs['times'], kwargs['article_nums']
        
    # 전체 기사 개수, 각 기사 제목과 본문마다 전처리 진행
    total_news_cnt = len(titles)

    # 기사 내 사진 출처 텍스트 제거 정규식
    content_pattern = re.compile(r'\n*\s*Photo.*\n\s*')
    # 기타 개행문자 제거 정규식
    backslash_pattern = re.compile(r'[\r\n\xa0\u200b\t]+')
    # 문장부호 제거 정규식
    symbol_pattern = re.compile(r'[^\w\s]')
    
    # 년.월.일 형식 날짜 데이터 추출
    time_pattern = re.compile(r'\w{4}.\w{2}.\w{2}')
    
    # 전처리 결과 리스트
    #preprocessed_titles = []
    #preprocessed_contents = []
    #preprocessed_times = []    
    
    json_list = []
    for i in range(total_news_cnt):
        sub_json = {}
                
        # i번째 기사 본문 사진 출처 텍스트 제거
        content = content_pattern.sub('', contents[i])
        # 출처를 제거한 텍스트에서 i번째 기사 개행문자 제거
        content = backslash_pattern.sub(' ', content)
        content = symbol_pattern.sub(' ', content)
        
        write_time = time_pattern.findall(times[i])[0]
        
        sub_json['article_index'] = article_nums[i]
        sub_json['write_time'] = write_time
        sub_json['title'] = titles[i]
        sub_json['content'] = content
        #preprocessed_titles.append(title)
        #preprocessed_contents.append(content)
        
        logging.info(sub_json)
        json_list.append(sub_json)
        
        
    return json_list

with DAG(
    dag_id='CollectArticleDatas',
    start_date=datetime(2023, 6, 1),
    catchup=False,
    schedule='59 2 * * *',
) as dag:
    news_link = get_latest_article_link()
    latest_news_idx = extract_news_num(news_link)
    extract_result_dict = extract_latest_articles(latest_news_idx)
    preprocess_data(titles=extract_result_dict['news_title'], 
                    contents=extract_result_dict['news_contents'], 
                    times=extract_result_dict['write_time'], 
                    article_nums=extract_result_dict['article_nums'])
    