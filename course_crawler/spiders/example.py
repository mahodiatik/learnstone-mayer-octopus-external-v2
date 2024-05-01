"""
@Author: John Doe
@Date: 01.01.2023.
"""

import re
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple

from functional import seq
from bs4 import BeautifulSoup, Tag

import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


# TODO: change spider name to match university
class ExampleSpider(scrapy.Spider):

    # TODO: change spider name to match university
    name = 'example'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
    
    university = 'University of Example'
    study_level = 'Graduate'

    # TODO: add university course catalogue to start_urls
    start_urls = [
        ''
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ExampleSpider, cls).from_crawler(crawler, *args, **kwargs)  # TODO: change spider name to match university
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        course_list = []
        for url in course_list:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course,
                                 dont_filter=True)

    def _get_title(self, soup: Tag) -> Optional[str]:
        try:
            title = None
        except AttributeError:
            title = None
        return title

    def _get_qualification(self, course: Tag) -> Optional[str]:
        try:
            qualification = None
        except AttributeError:
            qualification = None
        return qualification

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            locations = []
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description = None
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about = None
        except AttributeError:
            about = None
        return about

    def _get_tuitions(self, soup: BeautifulSoup) -> list:
        try:
            tuitions = []
        except AttributeError:
            tuitions = []
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_dates = []
        except AttributeError:
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            application_dates = []
        except AttributeError:
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements = None
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            english_language_requirements = []
        except (AttributeError, KeyError):
            english_language_requirements = []
        return english_language_requirements

    def _get_modules(self, soup: BeautifulSoup) -> List[dict]:
        try:
            modules = []
        except AttributeError:
            modules = []
        return modules

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        link = response.url
        title = self._get_title(soup)
        study_level = self.study_level
        qualification = self._get_qualification(soup)
        university = self.university
        locations = self._get_locations(soup)
        description = self._get_description(soup)
        about = self._get_about(soup)
        tuitions = self._get_tuitions(soup)
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)
        modules = self._get_modules(soup)

        yield {
            'link': link,
            'title': title,
            'study_level': study_level,
            'qualification': qualification,
            'university_title': university,
            'locations': locations,
            'description': description,
            'about': about,
            'tuitions': tuitions,
            'start_dates': start_dates,
            'application_dates': application_dates,
            'entry_requirements': entry_requirements,
            'language_requirements': language_requirements,
            'modules': modules
        }


def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(ExampleSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
