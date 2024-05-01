"""
@Author: Md. Mahodi Atik Shuvo
@Date: 2024-03-02
"""

import os
import re
import sys
from pathlib import Path
from datetime import datetime
from string import ascii_uppercase
from typing import List, Optional, Tuple

from functional import seq
from bs4 import BeautifulSoup, Tag
import requests
import json

import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class NewcastleSpider(scrapy.Spider):

    name = 'newcastle'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

    university = 'Newcastle University'
    study_level = 'Graduate'

    HOME_URL = 'https://www.ncl.ac.uk'

    language_certificates = {}

    # course_directory = 'https://www.ncl.ac.uk/postgraduate/degrees/'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(NewcastleSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        # yield scrapy.Request(url='https://www.nottingham.ac.uk/studywithus/international-applicants/english-language.aspx',
        yield scrapy.Request(url='https://www.ncl.ac.uk/postgraduate/degrees/',
                             callback=self.parse_course_list)

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser',
                             from_encoding='utf-8')

        # find all the list items with the class "courseSearchResults__courseName"
        course_list_items = soup.find_all(
            'li', class_='courseSearchResults__courseName')
        for course in course_list_items:
            course_url = course.a['href']
            course_name = course.a.text
            yield scrapy.Request(url=course_url,
                                 callback=self.parse_course,
                                 meta={
                                     'title': course_name,
                                     #  'playwright': True
                                 })

    def _get_title(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            title = soup.find(
                'meta', attrs={"name": "ncl_course_title"})['content']
        except (AttributeError, KeyError):
            title = None
        return title

    def _get_qualification(self, data: dict):
        try:
            qualification=set()
            course_data = data['CourseWebData']  
            for d in course_data:
                qualification.add(d['web_course_qualification'])
        except (AttributeError, KeyError):
            qualification = None
        return qualification

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            locations = []
            school = soup.find(attrs={'name': 'ncl_course_tertiary_classification'})[
                'content']
            if school == '':
                school = soup.find(attrs={'name': 'ncl_course_secondary_classification'})[
                    'content']
            if school != '':
                locations.append(school + ', Newcastle University')
        except AttributeError:
            locations = ['Newcastle University']
        return locations

    def _get_study_modes(self, tag) -> List[str]:
        try:
            study_mode_section = seq(tag.select('.courseInfo h4'))\
                .find(lambda x: x.text.strip() == 'Study mode').next_sibling.next_sibling
            study_modes = seq(study_mode_section.select('.possibleMultiple'))\
                .map(lambda x: x.text.strip())\
                .to_list()
        except (KeyError, AttributeError):
            study_modes = []
        return study_modes

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description = soup.find(attrs={'name': 'description'})['content']
        except (KeyError, AttributeError):
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about = ''
            about_head = seq(soup.select('h2')).find(
                lambda x: x.text.strip().lower() == 'overview'
            )
            head = about_head.nextSibling
            while (head.name == 'p' or head.name == 'ul' or head == '\n'):
                about = about + str(head)
                head = head.nextSibling
        except AttributeError:
            about = None
        return about

    # Get tuitions data from API

    def _get_tuitions_from_api(self, data: dict,parsed_qualification) -> List[dict]:
        tuitions = []
        try:
            course_data = data['CourseWebData']  
            for d in course_data:
                qualification = d['web_course_qualification']
                if (qualification.lower().find(parsed_qualification.lower())!=-1):
                    studymode = d['ft_pt']
                    fees_uk = d['fees_uk']
                    fees_intl = d['fees_int']
                    duration = d['course_length'] + ' Year'
                    tuitions.append({
                        "study_mode": studymode,
                        "duration": duration,
                        "student_category": "UK",
                        "fee": fees_uk
                    })
                    tuitions.append({
                        "study_mode": studymode,
                        "duration": duration,
                        "student_category": "international",
                        "fee": fees_intl
                    })
        except (KeyError, TypeError):
            tuitions = []
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_dates=[]
            dates=soup.select("div.startInfo li")
            for date in dates:
                value=date.text.strip()
                if(value!=""):
                    start_dates.append(value)
        except (KeyError, IndexError,AttributeError):
            start_dates = []
        return start_dates

    # The application dates are not available most of the time.
    # justing getting the field labeled 'closing_date' and the following field as aplication dates
    def _get_application_dates(self, data: dict) -> List[str]:
        try:
            application_dates = []
            application_info = data['ApplicationInfoRepositoriesData'][0]
            application_info_text = str(application_info)
            month_format = re.compile(r'January|February|March|April|May|June|July|August|September|October|November|December')
            dates = month_format.findall(application_info_text)
            for date in dates:
                application_dates.append(date)
        except (IndexError, KeyError):
            application_dates = []
        return application_dates

    '''
        extract entry requrements
        and english language requirements
        from the api response data.
    '''

    def _get_entry_requirements_from_api(self, data: dict) -> Optional[str]:
        try:
            entry_requirements_data = data['EntranceRequirementsData']
            entry_requirements = [d['academic_entry_requirement_information']
                                  for d in entry_requirements_data]
            entry_requirement = entry_requirements[0]
        except (KeyError, IndexError):
            entry_requirement = None
        return entry_requirement

    def _get_english_language_requirements_from_api(self, data: dict) -> List[dict]:
        english_language_requirements = []
        try:
            language_requirements_data = data['CourseLanguageBand'][0]
            for test in language_requirements_data:
                requirement = {
                    'language': 'English',
                    'test': test,
                    'score': language_requirements_data[test]
                }
                english_language_requirements.append(requirement)
        except (KeyError, IndexError):
            english_language_requirements = []
        return english_language_requirements

    def _get_modules(self, soup: BeautifulSoup,parsed_qualification) -> List[dict]:
        try:
            modules=[]
            try:
                modules_section = soup.find('section', class_='modules')
                if (modules==[]):
                    module_types = modules_section.select('tbody')
                    for module_t in module_types:
                        degree_text=module_t.find_previous('a', class_='dropDownHeading').get_text().lower()
                        if(degree_text.lower().find(parsed_qualification.lower())!=-1):
                            _type = module_t.find('th').text
                            module_list = module_t.find_all('a')
                            for module in module_list:
                                modules.append({
                                    'type': _type,
                                    'title': module.text.strip(),
                                    'link': module['href']
                                })
            except:
                pass
            try:
                if(modules==[]):
                    module_seclector=soup.select("#what-you-learn div div section div div li")
                    for module in module_seclector:
                        degree_text=module.find_previous('a', class_='dropDownHeading').get_text()
                        if(degree_text.lower().find(parsed_qualification.lower())!=-1):
                            title=module.text.strip()
                            try:
                                type_text=module.findPrevious("strong").text.lower()
                            except:
                                try:
                                    type_text=module.findPrevious("h5").text.lower()
                                except:
                                    type_text=module.findPrevious("h4").text.lower()
                            if(type_text.lower().find("optional")!=-1):
                                type="Optional"
                            else:
                                type="Compulsory"
                            try:
                                link=module.select_one('a').get("href")
                            except:
                                link=""
                            modules.append({
                                'type': type,
                                'title': title,
                                'link': link
                            })
            except:
                pass
            try:
                if(modules==[]):
                    module_seclector=soup.select("#what-you-learn div div section section div div  p a")
                    for module in module_seclector:
                        degree_text=module.find_previous('a', class_='dropDownHeading')
                        if(degree_text.lower().find(parsed_qualification.lower())!=-1):
                            title=module.text.strip()
                            try:
                                type_text=module.findPrevious("strong").text.lower()
                            except:
                                try:
                                    type_text=module.findPrevious("h5").text.lower()
                                except:
                                    type_text=module.findPrevious("h4").text.lower()

                            if(type_text.find("optional")!=-1):
                                type="Optional"
                            else:
                                type="Compulsory"
                            link=module.get("href")
                            modules.append({
                                'type': type,
                                'title': title,
                                'link': link
                            })
            except:
                pass

            #if course has only degree then the qulification won't be checked
            degrees = soup.find('meta', attrs={"name": "ncl_course_qualification"})['content']
            if(degrees==""):
                text=soup.find('meta', attrs={"name": "ncl_course_title"})['content']
                degrees=text.split()[-1]
            splited = [degree.strip() for degree in degrees.split(",")]
            if(len(splited)==1 and modules==[]):
                try:
                    modules_section = soup.find('section', class_='modules')
                    if (modules==[]):
                        module_types = modules_section.select('tbody')
                        for module_t in module_types:
                            _type = module_t.find('th').text
                            module_list = module_t.find_all('a')
                            for module in module_list:
                                modules.append({
                                    'type': _type,
                                    'title': module.text.strip(),
                                    'link': module['href']
                                })
                except:
                    pass
                try:
                    if(modules==[]):
                        module_seclector=soup.select("#what-you-learn div div section div div li")
                        for module in module_seclector:
                            title=module.text.strip()
                            try:
                                type_text=module.findPrevious("strong").text.lower()
                            except:
                                try:
                                    type_text=module.findPrevious("h5").text.lower()
                                except:
                                    type_text=module.findPrevious("h4").text.lower()
                            if(type_text.lower().find("optional")!=-1):
                                type="Optional"
                            else:
                                type="Compulsory"
                            try:
                                link=module.select_one('a').get("href")
                            except:
                                link=""
                            modules.append({
                                'type': type,
                                'title': title,
                                'link': link
                            })
                except:
                    pass
                try:
                    if(modules==[]):
                        module_seclector=soup.select("#what-you-learn div div section section div div  p a")
                        for module in module_seclector:
                            title=module.text.strip()
                            try:
                                type_text=module.findPrevious("strong").text.lower()
                            except:
                                try:
                                    type_text=module.findPrevious("h5").text.lower()
                                except:
                                    type_text=module.findPrevious("h4").text.lower()

                            if(type_text.find("optional")!=-1):
                                type="Optional"
                            else:
                                type="Compulsory"
                            link=module.get("href")
                            modules.append({
                                'type': type,
                                'title': title,
                                'link': link
                            })
                except:
                    pass
                
                


        except (AttributeError, KeyError):
            modules = []
        return modules

    '''
        The course pages are dynamically loaded.
        Taking advantage of the api calls made in the page for dynamic data loading
            get XHR fetches:
                Inspect >> network >> [filter xhr]
            get the CURL of the xhr
            converted curl to python request snippet using this site: [https://formatter.xyz/curl-to-python-converter]
            make api requests using requests
        A lot of required data can be found in the api response
    '''

   # academic year is required for making api call
    def _get_academic_year(self, soup: BeautifulSoup):
        try:
            academic_year = soup.find(
                'meta', attrs={"name": "ncl_course_fees_year"})['content']
        except (AttributeError, KeyError):
            academic_year = '2023'
        return academic_year

    # get course codes required for making api calls
    def _get_course_code(self, soup: BeautifulSoup) -> Optional[Tuple]:
        try:
            course_code = soup.find(
                'meta', attrs={"name": "ncl_course_code"})['content']
            additional_codes = soup.find(
                'meta', attrs={"name": "ncl_additional_codes"})['content'].split(',')
            for code in additional_codes:
                if code.lower() == course_code.lower():
                    additional_codes.remove(code)
        except (AttributeError, KeyError):
            course_code = None
            additional_codes = []
        return (course_code, additional_codes)

    '''
        get api data which is used for dynamic content loading
    '''

    def _get_data_from_api(self, course_code: str, additional_codes: List[str], academic_year=2023) -> dict:
        course_codes_str1 = course_code
        for code in additional_codes:
            course_codes_str1 += '%2C' + code
        course_codes_str2 = ','.join([course_code] + additional_codes)

        api1 = 'https://includes.ncl.ac.uk/cmswebservices/mcicache/wscache.php?url=https%3A%2F%2Fmci.ncl.ac.uk%2Fpublic%2Fmultiple%3Fcourse_codes%3D{course_codes_str1}%26academic_year%3D{year}&schema_ref=multiple_pg&cache_path={course_codes_str2}'.format(
            course_codes_str1=course_codes_str1, course_codes_str2=course_codes_str2, year=academic_year)
        api2 = 'https://includes.ncl.ac.uk/cmswebservices/mcicache/wscache.php?url=https%3A%2F%2Fmci.ncl.ac.uk%2Fpublic%2Fdominant%3Fcourse_code%3D{course_code}%26academic_year%3D{year}&schema_ref=dominant_pg&cache_path={course_code}'.format(
            course_code=course_code, year=academic_year)
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9,bn;q=0.8',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Origin': 'https://www.ncl.ac.uk',
            'Referer': 'https://www.ncl.ac.uk/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.35',
            'sec-ch-ua': '"Microsoft Edge";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        data = requests.get(api1, headers=headers).json()
        data2 = requests.get(api2, headers=headers).json()
        # combine the two api data and return
        data.update(data2)
        # Save the data as JSON
        # with open('course.json', 'a') as file:
        #     json.dump(data, file)

        return data

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser',
                             from_encoding='utf-8')
        # fees_tables = soup.select('.feesTable')
        # print(len(fees_tables))
        # print(str(soup))

        # get course data from api
        (course_code, additional_codes) = self._get_course_code(soup)
        academic_year = self._get_academic_year(soup)
        api_data = self._get_data_from_api(
            course_code=course_code, additional_codes=additional_codes, academic_year=academic_year)
        link = response.url
        title = self._get_title(soup)
        study_level = self.study_level
        qualifications = self._get_qualification(api_data)
        length=len(qualifications)
        university = self.university
        locations = self._get_locations(soup)
        description = self._get_description(soup)
        about = self._get_about(soup)
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(api_data)
        entry_requirements = self._get_entry_requirements_from_api(api_data)
        language_requirements = self._get_english_language_requirements_from_api(
            api_data)
        for qualification in qualifications:
            modules = self._get_modules(soup,qualification)
            tuitions = self._get_tuitions_from_api(api_data,qualification)

            if any([x.text.strip() in ["Suspended course", "Course withdrawal", "Course suspension", "Course suspended", "Withdrawn course"]
                    for x in soup.select("h2")]):
                pass
            else:
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
                    'modules': modules,
                }


def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(NewcastleSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
