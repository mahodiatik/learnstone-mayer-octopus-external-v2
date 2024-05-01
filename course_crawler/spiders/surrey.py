"""
@Author: Michael Milinković
@Date: 06.11.2022.
"""
import os
import re
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


class SurreySpider(scrapy.Spider):

    name = 'surrey'
    university = 'University of Surrey'
    study_level = 'Graduate'

    student_category_map = {'UK': 'uk', 'Overseas': 'international'}

    english_language_certificate_map = {}

    start_urls = [
        'https://www.surrey.ac.uk/postgraduate'
    ]

    output_path = os.path.join('..', 'data', 'courses', 'output') if os.getcwd().endswith('spiders') \
        else os.path.join('course_crawler', 'data', 'courses', 'output')

    # Overrides configuration values defined in course_crawler/settings.py
    custom_settings = {
        'FEED_URI': Path(f"{output_path}/{name}/"
                         f"{name}_graduate_courses_{datetime.today().strftime('%Y-%m-%d')}.json")
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SurreySpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        output_path = os.path.join('..', 'data', 'courses', 'output') if os.getcwd().endswith('spiders') \
            else os.path.join('course_crawler', 'data', 'courses', 'output')
        Path(f"{output_path}/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        # English language requirements
        yield scrapy.Request(
            url='https://www.surrey.ac.uk/apply/international/english-language-requirements',
            callback=self.parse_surrey_english_requirements)

        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)

    def parse_surrey_english_requirements(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        certificates = {}

        ielts_equivalents_section = seq(soup.select('h2'))\
            .find(lambda x: x.text.strip() == 'IELTS equivalents').next_sibling.next_sibling

        ielts_scores = seq(ielts_equivalents_section.select('thead th')[1:])\
            .map(lambda x: tuple(sorted(re.findall(r'\d\.\d', x.text.strip()))))\
            .to_list()
        ielts_score_map = {k + 1: v for k, v in enumerate(ielts_scores)}

        for score in ielts_scores:
            certificates[score] = []

        for row in ielts_equivalents_section.select('tbody')[1].select('tr'):
            cells = row.select('td')
            certificate, scores = cells[0].text.strip(), seq(cells[1:]).map(lambda x: x.text.strip()).to_list()

            for idx, cell in enumerate(scores):
                certificates[ielts_score_map[idx+1]].append({
                    'language': 'English',
                    'test': certificate,
                    'score': cell
                })

        self.english_language_certificate_map = certificates

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        course_list = soup.select('.view-content a')
        for course in course_list:
            url = f"https://www.surrey.ac.uk{course['href']}" if not course['href'].startswith('http') else course['href']
            title = self._get_title(course)
            qualification = self._get_qualification(course)

            yield scrapy.Request(url=url,
                                 callback=self.parse_course,
                                 dont_filter=True,
                                 meta={
                                     'title': title,
                                     'qualification': qualification
                                 })

    def _get_title(self, soup: Tag) -> Optional[str]:
        try:
            title = soup.select_one('span').text.strip()
        except AttributeError:
            title = None
        return title

    def _get_qualification(self, course: Tag) -> Optional[str]:
        try:
            course.select_one('span').decompose()
            qualification = course.text.strip()
        except AttributeError:
            qualification = None
        return qualification

    def _get_study_mode(self, course: Tag) -> Optional[List[str]]:
        try:
            study_mode = None
            for section in course.select('.type'):
                match = re.match(r'Mode of study:\s([\w\s/\-]+)', section.text)
                if not match:
                    continue
                study_mode = match.group(1)
                study_mode = study_mode.replace('FT', 'full-time').replace('PT', 'part-time')
                study_mode = seq(study_mode.split('/'))\
                    .map(lambda x: x.strip())\
                    .to_list()
                break
        except AttributeError:
            study_mode = None
        return study_mode

    def _get_locations(self, soup: BeautifulSoup) -> Optional[List[str]]:
        try:
            location_section = soup.select('section')[-1].select_one('.col-md-4')
            locations = [location_section.select('strong')[1].text.strip()]
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            what_you_will_study_section = seq(soup.select('h2'))\
                .find(lambda x: x.text.strip() == 'What you will study').next_sibling
            description = what_you_will_study_section.text.strip().split('.')[0]
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            why_choose_this_course_section = seq(soup.select('h2')) \
                .find(lambda x: x.text.strip() == 'Why choose this course').next_sibling

            what_you_will_study_section = seq(soup.select('h2')) \
                .find(lambda x: x.text.strip() == 'What you will study').next_sibling
            about = f"{str(why_choose_this_course_section)} {str(what_you_will_study_section)}"
        except AttributeError:
            about = None
        return about

    # TODO: some courses have multiple start dates and different fees associated with them
    #  (currently only one of the fee sets is scraped)
    #  (e.g. https://www.surrey.ac.uk/postgraduate/strategic-marketing-msc#fees)
    def _get_tuitions(self, soup: BeautifulSoup) -> list:
        try:
            tuitions = []

            section = soup.select('.pg-fees')[-1]
            subsections = section.select('.views-field-field-study-mode')
            for subsection in subsections:
                try:
                    study_mode, duration = seq(subsection.select_one('p').text.strip().split(' - '))\
                        .map(lambda x: x.strip())\
                        .to_list()
                except ValueError:
                    continue

                for fee_section in subsection.select('dl'):
                    student_category = self.student_category_map[fee_section.select_one('dt').text.strip()]

                    fee = fee_section.select_one('dd').text.strip()
                    if fee.endswith('*'):
                        replacement = soup.select_one('.credit-container').text.strip().replace('*', '')
                        fee = fee.replace('*', replacement)

                    tuitions.append({
                        'study_mode': study_mode,
                        'duration': duration,
                        'student_category': student_category,
                        'fee': fee
                    })
        except AttributeError:
            tuitions = []
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_dates = []
            sections = soup.select('section')[0].select('dl')
            for section in sections:
                start_date = re.match(r'Start date: (.*)', section.select_one('dt').text.strip()).group(1).strip()
                start_dates.append(start_date)
        except AttributeError:
            start_dates = []
        return start_dates

    # TODO: maybe map application dates to corresponding start dates?
    def _get_application_dates(self, soup: BeautifulSoup) -> List:

        def _format_application_date(date) -> Tuple[int, int]:
            month_str, year_str = date.split()[2:]
            month = datetime.strptime(month_str, '%B')
            year = int(year_str)
            return year, month.month
        try:
            dates = set()
            for application_date_row in soup.select('.course-row'):
                start_date = application_date_row.select_one('.startdate').text.strip()
                closing_date = application_date_row.select('p')[-1].text.strip().split(':')[1].strip()

                dates.add((start_date, closing_date))

            application_dates = seq(sorted(dates, key=lambda x: _format_application_date(x[1])))\
                .map(lambda x: x[1]) \
                .to_list()
            # .map(lambda x: {'semester': x[0], 'deadline': x[1]})\
        except AttributeError:
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements_section = seq(soup.select('h2'))\
                .find(lambda x: x.text.strip() == 'Entry requirements').next_sibling
            entry_requirements = " ".join(seq(entry_requirements_section.select('p')).map(lambda x: str(x)))
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    # TODO: extract requirements from table on course page
    #  (e.g. https://www.surrey.ac.uk/postgraduate/investment-management-msc)
    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        """
        University of Surrey maps IELTS scores to other standard English language proficiency tests.
        See -> https://www.surrey.ac.uk/apply/international/english-language-requirements
        """
        try:
            english_language_requirements = []
            language_requirements_section = seq(soup.select('h2'))\
                .find(lambda x: x.text.strip() == 'English language requirements').next_sibling

            ielts_test_title = language_requirements_section.select_one('strong').text.strip().rstrip(':')

            language_requirements_section.select_one('strong').decompose()
            ielts_test_score = language_requirements_section.select_one('p').text.strip()

            english_language_requirements.append({
                'language': 'English',
                'test': ielts_test_title,
                'score': ielts_test_score
            })

            ielts_test_score = tuple(sorted(re.findall(r'\d\.\d', ielts_test_score)))
            if ielts_test_score in self.english_language_certificate_map:
                english_language_requirements.extend(self.english_language_certificate_map[ielts_test_score])
        except (AttributeError, KeyError):
            english_language_requirements = []
        return english_language_requirements

    # TODO: scrape multiple module lists based on study mode
    #  (only first set of modules is scraped, usually corresponds to full-time study mode)
    #  (e.g. https://www.surrey.ac.uk/postgraduate/investment-management-msc#structure)
    def _get_modules(self, soup: BeautifulSoup) -> List[dict]:
        try:
            modules = []
            module_section = soup.select_one('.table-responsive')
            for module in module_section.select('tbody tr'):
                title, type, _ = module.select('td')

                modules.append({
                    'type': type.text.strip(),
                    'title': title.text.strip(),
                    'link': title.select_one('a')['href'].strip()
                })
        except AttributeError:
            modules = []
        return modules

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        link = response.url
        title = response.meta['title']
        study_level = self.study_level
        qualification = response.meta['qualification']
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


if __name__ == "__main__":
    cp = CrawlerProcess(get_project_settings())

    cp.crawl(SurreySpider)
    cp.start()
