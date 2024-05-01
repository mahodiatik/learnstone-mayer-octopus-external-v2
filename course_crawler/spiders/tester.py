from datetime import datetime
import json
import os
import re
from pathlib import Path
from typing import Dict, List
from urllib.parse import parse_qs
from bs4 import BeautifulSoup, Tag
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
        
        