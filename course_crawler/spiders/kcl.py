"""
Mahodi Atik Shuvo
Date: 01-04-2024
"""

import os
import re
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional

from functional import seq
from bs4 import BeautifulSoup, Tag

import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.reactor import install_reactor


class KCLSpider(scrapy.Spider):

    name = 'kcl'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

    university = 'King\'s College London'
    study_level = 'Graduate'

    language_certificates = {}

    start_urls = [
        'https://www.kcl.ac.uk/search/courses?level=postgraduate-taught'
    ]

    # Overrides configuration values defined in course_crawler/settings.py
    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler"
        }
    }

    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(KCLSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        yield scrapy.Request(
            url='https://www.kcl.ac.uk/study/postgraduate-taught/how-to-apply/entry-requirements/english-language-requirements',
            callback=self.parse_english_language_requirements
        )

    def parse_english_language_requirements(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        def _extract_requirements(band: Optional[str], table: Tag):
            if not band:
                return

            certificates = {}
            for row in table.select('tbody tr'):
                for a in row.select('a'):
                    a.decompose()

                test_title = row.select_one('strong').text.strip()
                test_score = row.select_one('td').text.strip()

                certificates[test_title] = test_score

            self.language_certificates[band] = certificates

        json_data = self._get_json_page_data(soup)

        band = None
        for entry in json_data['routing']['entry']['bodyContentComposer']:
            if entry['type'] == 'jumpLinksSection':
                band = re.findall(r'Band (\w+)', entry['value']['sectionTitle'])
                if not band:
                    continue
                band = band.pop()
            elif entry['type'] == 'markup':
                if 'value' not in entry or not entry['value'] or not entry['value'].startswith('<table'):
                    continue
                _extract_requirements(band, BeautifulSoup(entry['value'], 'html.parser', from_encoding='utf-8'))

        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_course_pages,
                dont_filter=True
            )

    def parse_course_pages(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        number_of_courses = soup.select_one('.SearchSummarystyled__SearchSummaryStyled-sc-1ez4k2h-0').select('strong')[1].text

        yield scrapy.Request(
            url=response.url,
            callback=self.parse_course_list
        )

        for i in range(1, int(number_of_courses) // 20 + 1):
            url = 'https://www.kcl.ac.uk/search/courses?coursesPage=' + str(i) + '&level=postgraduate-taught'
            yield scrapy.Request(
                url=url,
                callback=self.parse_course_list
            )

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        courses = soup.select_one('.gOjlCe').select('article')[1:]
        for course in courses:
            url = course.find_parent()['href']
            with open(f"urls.txt", "a") as f:
                f.write(url + '\n')


            yield scrapy.Request(
                url=url,
                callback=self.parse_course,
                dont_filter=True,
                meta={"playwright": True}
            )

    def _get_json_page_data(self, soup: BeautifulSoup) -> dict:
        try:
            script = seq(soup.select('script')).find(lambda x: 'window.REDUX_DATA' in str(x))
            json_str = re.match(r'.*REDUX_DATA\s\=\s(.*)$', script.text).group(1)
            json_str = json_str.replace(':undefined', ":null")

            json_data = json.loads(json_str)
        except AttributeError:
            json_data = None
        return json_data

    def _get_title(self, json_data: dict) -> Optional[str]:
        try:
            title = json_data['routing']['entry']['entryTitle']
        except AttributeError:
            title = None
        return title

    def _get_qualifications(self, soup):
        try:
            qualifications = []
            text= soup.select_one("div.Columnstyled__ColumnStyled-f3ck65-0.cxWyvB h1").text
            qualification_pattern= re.compile(r'(MSc|PG Dip|PGDip|PG Cert|PGCert|MA|MRes|PGCE|MPH|MClinDent|MMus|BSc|DClinDent|LLM|Grad Cert|MBA)')
            list= qualification_pattern.findall(text)
            for qualification in list:
                qualifications.append(qualification)
        except (AttributeError, TypeError):
            qualifications = []
        return qualifications

    def _get_locations(self, json_data: dict) -> List[str]:
        try:
            locations = seq(json_data["routing"]["entry"]["campuses"])\
                .map(lambda x: x["address"])\
                .to_list()
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, json_data: dict) -> Optional[str]:
        try:
            course_information = json_data["routing"]["entry"]["courseInformation"]
            description = f"{course_information.split('.')[0]}."
        except (AttributeError, TypeError):
            description = None
        return description

    def _get_about(self, json_data: dict) -> Optional[str]:
        try:
            course_information = json_data["routing"]["entry"]["courseInformation"]
            course_essentials = json_data["routing"]["entry"]["courseEssentials"]
            course_aims = "".join(seq(json_data["routing"]["entry"]["courseAims"])
                                  .map(lambda x: f"<li>{x}</li>")
                                  .to_list())

            about = f"<div><div>{course_information}</div><div>{course_essentials}</div><ul>{course_aims}</ul></div>"
        except IndexError:
            about = None
        return about

    def _get_tuitions(self, json_data: dict) -> list:                         
        try:
            mapper = {
                'ukFee': ('uk', 'Full-time'),
                'internationalFee': ('international', 'Full-time'),
                'ukPartTimeFee': ('uk', 'Part-time'),
                'internationalPartTimeFee': ('international', 'Part-Time')
            }

            # # TODO: extract durations
            tuitions = []
            try:
                for k in seq(mapper.keys()).filter(lambda x: x in json_data['routing']['entry']):
                    fee = json_data['routing']['entry'][k]
                    duration = json_data['routing']['entry']['duration']
                    if not fee:
                        continue

                    fee = re.findall(r'£\d+,\d+', fee).pop(0)

                    student_category, study_mode = mapper[k]
                    tuitions.append({
                        'study_mode': study_mode,
                        'duration': duration,
                        'student_category': student_category,
                        'fee': fee
                    })
            except:
                pass
            if(len(tuitions) == 0):
                try:
                    for k in seq(mapper.keys()).filter(lambda x: x in json_data['routing']['entry']['relatedCourses'][0]['relatedCourses'][0]):
                        fee=json_data['routing'] ['entry'] ['relatedCourses'][0]['relatedCourses'][0][k]
                        duration= json_data['routing']['entry']['relatedCourses'][0]['relatedCourses'][0]['duration']

                        if not fee:
                            continue

                        fee = re.findall(r'£\d+,\d+', fee).pop(0)
                
                        student_category, study_mode = mapper[k]
                        tuitions.append({
                            'study_mode': study_mode,
                            'duration': duration,
                            'student_category': student_category,
                            'fee': fee
                        })
                except:
                    pass
            if(len(tuitions) == 0):
                try:
                    for k in seq(mapper.keys()).filter(lambda x: x in json_data['routing']['entry']):
                        fee = json_data['routing']['entry']["furtherFeeInformation"]
                        duration = json_data['routing']['entry']['duration']
                        if not fee:
                            continue

                        fee = re.findall(r'£\d+,\d+', fee).pop(0)

                        student_category, study_mode = mapper[k]
                        tuitions.append({
                            'study_mode': study_mode,
                            'duration': duration,
                            'student_category': student_category,
                            'fee': fee
                        })
                except:
                    pass
            if(len(tuitions) == 0):
                try:
                    for k in seq(mapper.keys()).filter(lambda x: x in json_data['routing']['entry']):
                        fee = json_data['routing']['entry']["onlineCourseFeeInformation"]
                        duration = json_data['routing']['entry']['duration']
                        if not fee:
                            continue

                        fee = re.findall(r'&pound;(\d+,\d+)', fee).pop(0)

                        student_category, study_mode = mapper[k]
                        tuitions.append({
                            'study_mode': study_mode,
                            'duration': duration,
                            'student_category': student_category,
                            'fee': f'£{fee}'


                        })
                except:
                    pass
                
            
        except IndexError:
            tuitions = []

        return tuitions




    def _get_start_dates(self, json_data: dict) -> List[str]:
        try:
            start_dates = seq(json_data["routing"]["entry"]["startDates"]) \
                .map(lambda x: x["title"]) \
                .to_list()
        except AttributeError:
            start_dates = []
        return start_dates
    

    '''
    Default dates are described in the following api:
    ---------------------------------------------------------------------------------------------------------------------------
    curl 'https://www.kcl.ac.uk/api/delivery/projects/website/entries/72e8ecb1-123e-4621-bfef-0cbf249eb40e?linkDepth=1' \
    -H 'accept: */*' \
    -H 'accept-language: en-US,en;q=0.9,bn;q=0.8' \
    -H 'accesstoken: JjV9NgvYm8BQgTNx2AtThsRBeK5qZxArDnRc2SKrzYWzvsS6' \
    -H 'cookie: OptanonAlertBoxClosed=2024-03-28T08:51:00.450Z; _gid=GA1.3.509480863.1711833552; _fbp=fb.2.1711902939866.201974845; __hs_cookie_cat_pref=1:true_2:true_3:true; hubspotutk=338f7eef2923d6365b019b9516e5b2ec; __hssrc=1; _gcl_au=1.1.1626541155.1711902948; _hjSessionUser_3882840=eyJpZCI6IjNjZDAxM2Y3LWU1YjEtNTBlZC1hZjUzLWI4YTFmODJiMDk3NiIsImNyZWF0ZWQiOjE3MTE5MDI5NDExNzYsImV4aXN0aW5nIjp0cnVlfQ==; __hstc=102787167.338f7eef2923d6365b019b9516e5b2ec.1711902945790.1711914310426.1711924218847.3; _ga_2B2LNW4G7M=GS1.1.1711947718.6.0.1711947718.0.0.0; _ga_MB7LPD0MRQ=GS1.1.1711950710.7.1.1711952107.0.0.0; _ga=GA1.3.1650481729.1711615941; _gat_gtag_UA_228896_1=1; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Apr+01+2024+13%3A51%3A53+GMT%2B0600+(Bangladesh+Standard+Time)&version=202402.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=1aec5d0b-87c7-45e8-8e05-b61c16202a81&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&hosts=H105%3A1%2CH148%3A1%2CH103%3A1%2CH130%3A1%2CH24%3A1%2CH104%3A1%2CH1%3A1%2CH201%3A1%2CH179%3A1%2CH19%3A1%2CH20%3A1%2CH144%3A1%2CH181%3A1%2CH117%3A1%2CH33%3A1%2CH48%3A1%2CH195%3A1%2CH9%3A1%2CH11%3A1%2CH106%3A1%2CH204%3A1%2CH57%3A1%2CH58%3A1%2CH59%3A1%2CH202%3A1%2CH21%3A1%2CH114%3A1%2CH115%3A1%2CH116%3A1%2CH23%3A1%2CH149%3A1%2CH173%3A1%2CH203%3A1%2CH150%3A1%2CH185%3A1%2CH126%3A1%2CH161%3A1%2CH127%3A1%2CH156%3A1%2CH128%3A1%2CH140%3A1%2CH198%3A1%2CH180%3A1%2CH62%3A1%2CH200%3A1%2CH187%3A1%2CH25%3A1%2CH26%3A1&genVendors=&geolocation=BD%3BC&AwaitingReconsent=false' \
    -H 'dnt: 1' \
    -H 'referer: https://www.kcl.ac.uk/study/postgraduate-taught/courses/applied-neuroscience-msc' \
    -H 'sec-ch-ua: "Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"' \
    -H 'sec-ch-ua-mobile: ?0' \
    -H 'sec-ch-ua-platform: "Linux"' \
    -H 'sec-fetch-dest: empty' \
    -H 'sec-fetch-mode: cors' \
    -H 'sec-fetch-site: same-origin' \
    -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    ---------------------------------------------------------------------------------------------------------------------------
    As there are only three values in all cases and parsing them from this api takes much time, I put them manually in the default_dates list
    '''

    def _get_application_dates(self, json_data: dict,soup: BeautifulSoup) -> List[str]:
        try:
            try:
                application_dates_section = json_data['routing']['entry']['applicationClosingDateInfoOverride']
                application_dates = re.findall(r'(\d+ \w+ \d+) \(23:59 UK time\)', application_dates_section)
            except:
                application_dates = []
            text= soup.select("h4")
            for i in text:
                if(i.text =="Application closing date guidance"):
                    data= i.find_next("div").text
                    try:
                        date_pattern = r'\b(?:\d{1,2}(?:st|nd|rd|th)?(?:\s\w+)? \w+ \d{4})\b'
                        matches = re.findall(date_pattern, data)
                        for match in matches:
                            application_dates.append(match)
                    except:
                        application_dates = []
            default_dates=['10 March 2024', '26 July 2024', '26 August 2024'] 
            if(len(application_dates) == 0):
                application_dates=default_dates
        except (AttributeError, TypeError):
            application_dates=[]
        return application_dates

    def _get_entry_requirements(self, json_data: dict) -> Optional[str]:
        try:
            try:
                basic_requirements = json_data['routing']['entry']['entryRequirement']['pgtDefault']
                other_requirements = json_data['routing']['entry']['otherRequirements']

                entry_requirements = "".join(seq([basic_requirements, other_requirements]).filter(lambda x: x).to_list())
            except:
                entry_requirements = json_data['routing']['entry']['entryRequirement']
        except (AttributeError, TypeError):
            entry_requirements = ""
        return entry_requirements

    def _get_english_language_requirements(self, json_data: dict) -> List[dict]:
        try:
            english_language_requirements = []

            band = json_data["routing"]["entry"]["languageRequirementBand"]["title"]
            for test, score in self.language_certificates[band].items():
                english_language_requirements.append({
                    'language': 'English',
                    'test': test,
                    'score': score
                })
        except (AttributeError, KeyError, TypeError):
            english_language_requirements = []
        return english_language_requirements

    def _get_modules(self, json_data: dict) -> list:
        try:
            modules = []

            def _extract_modules(key: str, module_type: str) -> list:
                return seq(json_data['routing']['entry'][key])\
                    .filter(lambda x: x['type'] == 'modules')\
                    .map(lambda x: x['value'])\
                    .flatten()\
                    .map(lambda x: (re.sub(r'\(.*\)', '', x['entryTitle']).strip(), module_type))\
                    .to_list()

            core_modules = _extract_modules('requiredModules', 'Core')
            optional_modules = _extract_modules('optionalModules', 'Optional')

            for title, module_type in core_modules + optional_modules:
                modules.append({
                    'type': module_type,
                    'title': title,
                    'link': None
                })
        except AttributeError:
            modules = []
        return modules

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')


        json_data = self._get_json_page_data(soup)
        if(json_data):
            link = response.url
            title = self._get_title(json_data)
            study_level = self.study_level
            qualifications = self._get_qualifications(soup)
            university = self.university
            locations = self._get_locations(json_data)
            description = self._get_description(json_data)
            about = self._get_about(json_data)
            tuitions = self._get_tuitions(json_data)
            start_dates = self._get_start_dates(json_data)
            application_dates = self._get_application_dates(json_data,soup)
            entry_requirements = self._get_entry_requirements(json_data)
            language_requirements = self._get_english_language_requirements(json_data)
            modules = self._get_modules(json_data)

            for qualification in qualifications:
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
    cp.crawl(KCLSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
