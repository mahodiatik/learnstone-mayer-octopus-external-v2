from datetime import datetime
import json
import os
import re
from pathlib import Path
from typing import Dict, List
from urllib.parse import parse_qs
from bs4 import BeautifulSoup, Tag
from functional import seq
import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.http import HtmlResponse
from scrapy_playwright.page import PageMethod
from playwright.sync_api import sync_playwright, Playwright
import requests
import asyncio
from playwright.async_api import async_playwright, Playwright

'''
#playwright section----use if needed
async def call():
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless = True)
    page = await browser.new_page()

    await page.goto("https://www.sheffield.ac.uk/postgraduate/taught/courses/2024/advanced-clinical-practice-gp-mmedsci")
    content = await page.content()

    await browser.close()
    await playwright.stop()
    return content
content=asyncio.run(call())
soup = BeautifulSoup(content, "html.parser")

'''

# english_language_requirements = ""
# selector = soup.select("div#entry-requirements p")
# for i in selector:
#     next=i.find_next("h3")
#     if(next.text=="English language requirements"):
#         english_language_requirements+=str(i)
#     else:
#         break
# print(english_language_requirements)
        
link= 'https://www.exeter.ac.uk/study/postgraduate/courses/engineering/engmanagemsc/'
response= requests.get(link)
soup= BeautifulSoup(response.content, 'html.parser')

def _get_duration(soup: BeautifulSoup) -> dict:
    try:
        duration_dict = {}
        duration_section = soup.select_one('.exeter-course-duration')
        for item in duration_section.text.split('\n'):
            try:
                study_mode = re.findall(r'part time|full time|full-time|part-time', item.lower()).pop()
            except IndexError:
                study_mode = ''
            duration = item.replace(study_mode, '').strip()
            study_mode = study_mode.replace(' ', '-')

            duration_dict[study_mode] = duration
    except (AttributeError, IndexError):
        duration_dict = {}
    return duration_dict

def _get_tuitions(soup: BeautifulSoup) -> list:
    try:
        tuitions = []

        duration_dict =_get_duration(soup)
        if not duration_dict:
            return []

        for student_category in ['uk', 'international']:
            fees_section = seq(soup.select('#fees h3')).find(lambda x: student_category in x.text.lower())
            if not fees_section:
                fees_section = seq(soup.select('#Fees h3')).find(lambda x: student_category in x.text.lower())
            if not fees_section:
                fees_section = seq(soup.select('#fees h4')).find(lambda x: student_category in x.text.lower())
            if not fees_section:
                continue
            fees_section = fees_section.next_sibling.next_sibling
            print(fees_section)
            if fees_section.prettify().strip().find('<ul>') != -1:
                for fee in fees_section.find_all('li'):
                    ok=fee.text
                    try:
                        study_mode= re.findall(r'part time|full time|full-time|part-time', fee.text).pop()
                    except:
                        study_mode="full-time"
                    try:
                        duration= re.findall(r'\d+ year|\d+ years', fee.text).pop()
                    except:
                        duration="1 year"
                    fee=re.findall(r'£\d+,\d+', fee.text).pop()
                    tuitions.append({
                        'study_mode': study_mode,
                        'duration': duration,
                        'student_category': student_category,
                        'fee': fee
                    })
            else:
                delimiter = ';' if ';' in fees_section.text else '\n'
                for fee in fees_section.text.split(delimiter):
                    for study_mode, duration in duration_dict.items():
                        if study_mode in fee:
                            tuitions.append({
                                'study_mode': study_mode,
                                'duration': duration,
                                'student_category': student_category,
                                'fee': fee.replace(study_mode, '').strip()
                            })
                if tuitions==[]:
                    print(duration_dict)
                    for study_mode, duration in duration_dict.items():
                        if study_mode in fees_section.text.lower():
                            tuitions.append({
                                'study_mode': study_mode,
                                'duration': duration,
                                'student_category': student_category,
                                'fee': fees_section.text.lower().replace(study_mode, '').strip()
                            })
        if tuitions == []:
            for study_mode, duration in duration_dict.items():
                tuitions.append({
                                'study_mode': study_mode,
                                'duration': duration,
                                'student_category': 'All',
                                'fee': ''
                            })
    except AttributeError:
        tuitions = []
    return tuitions
ok= _get_tuitions(soup)
print(ok)
