"""
@Author: Michael Milinković
@Date: 07.12.2022.
"""

import os
import re
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


class ExeterSpider(scrapy.Spider):

    name = 'exeter'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

    university = 'University of Exeter'
    study_level = 'Graduate'

    language_certificates = {}
    application_dates = []

    start_urls = [
        'https://www.exeter.ac.uk/study/postgraduate/courses/'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ExeterSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        yield scrapy.Request(url='https://www.exeter.ac.uk/study/englishlanguagerequirements/',
                             callback=self.parse_english_language_requirements,
                             priority=10)

        yield scrapy.Request(url='https://www.exeter.ac.uk/study/postgraduate/applying/applicationdeadlines/',
                             callback=self.parse_application_dates,
                             priority=9)

        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list,
                                 priority=-1)

    def parse_application_dates(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        date_pattern = r'\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b'
        page_text = soup.select_one('#main-col').text
        self.application_dates = re.findall(date_pattern, page_text)
    
    def parse_english_language_requirements(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        title = soup.find('h1', text=re.compile('English language requirements'))
        base_url = 'https://www.exeter.ac.uk'
        for profile in title.find_next('ul').select('a'):
            yield scrapy.Request(url=base_url + profile['href'],
                             callback=self.parse_english_language_requirements_link,
                             priority=10)

    def parse_english_language_requirements_link(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        profile_title = soup.find('h2', text=re.compile('Profile')).text
        self.language_certificates[profile_title] = {}

        for table in soup.select('tbody'):
            for row in table.select('tr')[1:]:
                test_section = row.select('td')[0]
                score_section = row.select('td')[1]

                if test_section.select('a'):
                    test_title = test_section.select_one('a').text.strip()
                else:
                    test_title = test_section.text.strip()
                score = score_section.text.strip()

                self.language_certificates[profile_title][test_title] = score

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        course_list = seq(soup.select('a'))\
            .filter(lambda x: re.match(r"\/study\/postgraduate\/courses\/\w+\/\w+\/", x['href']))\
            .to_list()

        for course in course_list:
            link = f"https://www.exeter.ac.uk{course['href']}"

            yield scrapy.Request(url=link,
                                 callback=self.parse_course)

    def _get_title(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            script = seq(soup.select('script')).find(lambda x: 'credentialCategory' in str(x))
            qualification_dict = eval(script.text)

            title = qualification_dict['schema:name']
        except AttributeError:
            title = None
        return title

    def _get_qualification(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            script = seq(soup.select('script')).find(lambda x: 'credentialCategory' in str(x))
            qualification_dict = eval(script.text)

            qualification = qualification_dict['schema:educationalCredentialAwarded']['credentialCategory']
            qualification = qualification if qualification else None
            if not qualification:
                matches = re.findall(r'LLM|MRes|MSc|MPH|MA|PG Dip|PGDip|MEd|PGCert|PG Cert|MBA', qualification_dict['schema:name'])
                if matches:
                    qualification = matches.pop()
        except AttributeError:
            qualification = None
        return qualification

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            locations = []

            location_text = soup.select_one('.exeter-course-location').text.strip()
            location_text = location_text.replace('\xa0', ' ')

            for location in location_text.split(" and "):
                location = location.replace(', Exeter', '')
                if 'Campus' not in location:
                    location = f"{location} Campus"
                locations.append(location.strip())
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description_header = seq(soup.select('h2')).find(lambda x: 'Overview' in x.text).next_sibling.next_sibling
            description = description_header.select('li')[0].text.strip()
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about_elements = [str(seq(soup.select('h2')).find(lambda x: 'Overview' in x.text).next_sibling.next_sibling)]

            course_content_el = seq(soup.select('h2')).find(lambda x: 'Course content' in x.text).next_sibling
            while course_content_el is not None and course_content_el.next_sibling is not None:
                course_content_el = course_content_el.next_sibling
                if course_content_el.name and (course_content_el.name.startswith('h') or course_content_el.name == 'div'):
                    break
                if course_content_el.name:
                    about_elements.append(str(course_content_el))
            about = f"<div>{''.join(about_elements)}</div>"
        except AttributeError:
            about = None
        return about

    def _get_duration(self, soup: BeautifulSoup) -> dict:
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

    # TODO: capture differences in fee structure
    #  (e.g. https://www.exeter.ac.uk/study/postgraduate/courses/medicine/mphonline/#fees)
    #  (e.g. https://www.exeter.ac.uk/study/postgraduate/courses/medicine/neuroscience/#fees)
    def _get_tuitions(self,soup: BeautifulSoup) -> list:
        try:
            tuitions = []

            duration_dict =self._get_duration(soup)
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
                if fees_section.prettify().strip().find('<ul>') != -1:
                    for fee in fees_section.find_all('li'):
                        ok=fee.text
                        try:
                            study_mode= re.findall(r'part time|full time|full-time|part-time', fee.text).pop()
                        except:
                            study_mode="full-time" #full-time study mode is sometimes not mentioned in list items
                        try:
                            duration= re.findall(r'\d+ year|\d+ years', fee.text).pop()
                        except:
                            duration="1 year" #duration of full-time study mode is 1 year in every courses
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
    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_dates = seq(soup.select('select#programme-selector option')[1:])\
                .map(lambda x: x.text.strip())\
                .to_list()
            if start_dates == []:
                start_date = soup.select_one('#courseSummaryEntryYear').text
                if len(start_date.split()) > 1:
                    start_dates.append(start_date)

        except AttributeError:
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            application_dates = self.application_dates
        except (AttributeError, TypeError):
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements_section = seq(soup.select('h2'))\
                .find(lambda x: 'Entry requirements' in x.text).next_sibling.next_sibling
            if entry_requirements_section.name == 'h3':
                entry_requirements = entry_requirements_section.find_next('li').text
            else:
                entry_requirements = str(entry_requirements_section)
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            english_language_requirements = []

            profile = soup.find('strong', text=re.compile('Profile')).text
            for test, score in self.language_certificates[profile].items():
                english_language_requirements.append({
                    'language': 'English',
                    'test': test,
                    'score': score
                })
        except (AttributeError, KeyError):
            english_language_requirements = []
        return english_language_requirements

    def parse_module_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        try:
            h = 'h3'
            module_types = seq(soup.select('h3'))\
                .filter(lambda x: re.findall(r'\w+\smodules', x.text))\
                .map(lambda x: re.match(r'(.*)modules', x.text).group(1).strip())\
                .to_list()
            if module_types == []:
                h = 'h4'
                module_types = seq(soup.select('h4'))\
                .filter(lambda x: re.findall(r'\w+\smodules', x.text))\
                .map(lambda x: re.match(r'(.*)modules', x.text).group(1).strip())\
                .to_list()
            

            modules = []
            for module_type in module_types:
                module_section = seq(soup.select(h))
                for section in module_section:
                    if module_type in section.text:
                        table = section.find_next('table')
                        if table is not None:
                            for module in table.select('tr')[1:]:
                                cols = module.select('td')
                                if len(cols) != 3:
                                    continue
                                title = cols[1].text.strip()
                                new_module = {
                                    'type': module_type,
                                    'title': title,
                                    'link': None
                                }
                                if new_module not in modules:
                                    modules.append(new_module)
        except AttributeError:
            modules = []

        item = response.meta['item']
        item['modules'] = modules

        yield item

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        link = response.url
        title = self._get_title(soup)
        study_level = self.study_level
        qualifications = self._get_qualification(soup)

        if not qualifications or qualifications == 'Presessional':
            return
        for qualification in qualifications.split('/'):
            university = self.university
            locations = self._get_locations(soup)
            description = self._get_description(soup)
            about = self._get_about(soup)
            tuitions = self._get_tuitions(soup)
            start_dates = self._get_start_dates(soup)
            application_dates = self._get_application_dates(soup)
            language_requirements = self._get_english_language_requirements(soup)
            entry_requirements = self._get_entry_requirements(soup)
            item = {
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
                'language_requirements': language_requirements
            }

            try:
                module_section = soup.select_one('#course-content-accordion iframe')
                if module_section is not None:
                    module_section_link = soup.select_one('#course-content-accordion iframe')['src']
                    url = f'https:{module_section_link}'
                else:
                    url = response.url
                yield scrapy.Request(url=url,
                                    callback=self.parse_module_list,
                                    meta={'item': item})
            except TypeError:
                item['modules'] = []
                yield item


def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(ExeterSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
