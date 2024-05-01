"""
@Author: Muztoba Sinha
@Date: 10.04.2024.
"""
import re
import os
import sys
import requests
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

BASE_URL = 'https://www.st-andrews.ac.uk/'

class StAndrewsSpider(scrapy.Spider):

    name = 'standrews'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
    university = 'University of St Andrews'
    study_level = 'Graduate'
    lang_url = 'https://www.st-andrews.ac.uk/subjects/entry/language-requirements/postgraduate/'
    lang_mappings = {}

    start_urls = [
        'https://www.st-andrews.ac.uk/subjects/course-search/?collection=uosa-web-course--search&profile=_default&form=master&start_rank=1&num.ranks=10&sort=metatitle&mod=false&f.Year%7Cayrs=2023%2F4&query=%21null&f.tabs%7Ctype=Postgraduate&num_ranks=150'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(StAndrewsSpider, cls).from_crawler(crawler, *args, **kwargs)  
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def parse_language_mapping_links(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        table = soup.find('table')
        links = [a['href'] for a in table.find_all('a')]
        unique_links = list(set(links))
        for link in unique_links:
            yield scrapy.Request(url=BASE_URL+link, callback=self._get_language_mappings)
            
    def _get_language_mappings(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        link = response.url
        table = soup.find('table', class_='table-striped')
        rows = table.find_all('tr')[1:]
        lang_req = []
        for row in rows:
            test_name = row.find('th').text.strip()
            score_data = row.find_all('td')
            scores = [data.text.strip() for data in score_data]
            score = 'minimum component score: ' + scores[0] + ' minimum overall score: ' + scores[1]
            url_parts = link.split('/')
            lang_req.append({'language': 'English', 'test': test_name, 'score': score})
        self.lang_mappings[url_parts[-2]] = lang_req
    
    def start_requests(self):
        yield scrapy.Request(url=self.lang_url, callback=self.parse_language_mapping_links)
        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        results = soup.find_all('li', class_="search-result")

        for result in results:
            a = result.find('a', class_='search-result__link')
            link = a['href']
            title = a['title']
            duration = ""
            start_date = ""
            study_mode = ""
            dts = result.find_all('dt')

            for dt in dts:
                if dt.text == "Teaching mode":
                    study_mode = dt.find_next('dd').text
                    continue
                if dt.text == "Duration":
                    duration = dt.find_next('dd').text
                    continue
                if dt.text == "Start date":
                    start_date = dt.find_next('dd').text
            yield scrapy.Request(url=link,
                                 callback=self.parse_course,
                                 dont_filter=True,
                                 meta={
                                     'title': title,
                                     'study_mode': study_mode,
                                     'duration': duration,
                                     'start_date': start_date
                                 })

    def _get_title(self, soup: Tag) -> Optional[str]:
        try:
            title = None
            h1_tag = soup.select_one('h1.page-intro__heading')
            text = h1_tag.text.strip().split('(')
            title = text[0].strip()
        except AttributeError:
            title = None
        return title
    
    def _get_qualification(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            qualification = None
            h1_tag = soup.select_one('h1.page-intro__heading')
            if '(' not in h1_tag.text:
                if '-' in h1_tag.text:
                    return 'PGCert/PGDip/MSc' # https://www.st-andrews.ac.uk/subjects/marine-biology/sustainable-aquaculture/ only for this one outlier
                else:
                    return h1_tag.text.split(' ')[-1] # 
            text = h1_tag.text.strip().split('(')
            qf = text[-1].split(')')[0]
            qualification = qf
        except AttributeError:
            qualification = None
        return qualification


    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        # no locations in 'https://www.st-andrews.ac.uk/subjects/divinity/judaism-and-christianity-mlitt/' the @ thing handles this. 
        try:
            locations = []
            dd_tag = soup.select('dd.paired-values-list__value')
            text = dd_tag[-1].get_text(separator='\n', strip=True)
            lines = text.split('\n')
            line  = " ".join([l for l in lines])
            locations.append(line)
            if '@' in line:
                locations = []
        except:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description = None
            lead = soup.find('p', class_='lead')
            p = lead.next_sibling
            description = p.get_text(strip=True)
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about = None
            course_details_section = soup.find('section', id='course-details')
            course_details_text = course_details_section.get_text(strip=True)
            about = course_details_text.split('Modules')[0].strip()
            about += course_details_text.split('Teaching')[1].strip()
        except AttributeError:
            about = None
        return about

    #TODO: 'https://www.st-andrews.ac.uk/subjects/divinity/judaism-and-christianity-mlitt/' 'https://www.st-andrews.ac.uk/subjects/german/german-comparative-literature-mlitt/' 'https://www.st-andrews.ac.uk/subjects/tesol/tesol-dprof-august-and-january/'
    def _get_tuitions(self, qualification, duration, study_mode, soup: BeautifulSoup) -> list:
        try:
            tuitions = []
            # # study mode
            # study_mode = 'Full-Time'
            # h1_tag = soup.select_one('h1.page-intro__heading')
            # text = h1_tag.text.strip().split('(')
            # if 'online' in h1_tag.text.lower():
            #     study_mode = 'Part-Time/Online'

            # # duration
            # dd_tag = soup.select('dd.paired-values-list__value')
            # duration = ""
            # i = 0
            # while 'year' not in duration.lower():
            #     duration = dd_tag[i].text.strip()
            #     i+=1 

            if '(' in duration:
                pos = re.search(qualification, duration)
                # print(pos)
                if pos:
                    l = duration[:pos.start()]
                    r = duration[pos.start():]
                    if duration[pos.start()-1] == '(':
                        if l.startswith(qualification):
                            duration = r
                        else:
                            duration = l
                        duration = duration.split(')')[-1]

                    else:
                        split_text = duration[pos.start():].split('(')
                        duration = split_text[1].split(')')[0]
            # fees
            fee_sec = soup.find('section', id='fees')
            fees = fee_sec.find_all('p')
            h3 = fee_sec.find('h3', string=qualification.strip())
            uk_fee = ''
            int_fee = ''
            if h3:
                text = h3.find_next('p').get_text()
                if 'home' in text.lower():
                    int_fee = text.split('Overseas:')[1]
                    uk_fee = text.split('Overseas:')[0].split('Home:')[1]
                else:
                    int_fee = text
                    uk_fee = text
                    
            for fee in fees:
                text = fee.get_text()
                if 'home' in text.lower():
                    try:
                        uk_fee = text.split('£')[1]
                        # int_fee = uk_fee
                    except:
                        uk_fee = text
                elif 'overseas' in text.lower():
                    try:
                        int_fee = text.split('£')[1]
                    except:
                        int_fee = text
            if uk_fee == '':
                fees = fee_sec.find_all('li')
                for fee in fees:
                    text = fee.get_text()
                    txt = text.split()
                    txt = ''.join(txt)
                    if 'home' in text.lower():
                        uk_fee = text.split(':')[1].split('Overseas')[0]
                    elif 'overseas' in text.lower():
                        int_fee = text.split(':')[1]
                    elif qualification.lower() in txt.lower():
                        uk_fee = text
                        int_fee = uk_fee
                        if uk_fee.startswith('('):
                            dur = uk_fee.split('(')[1]
                            dur = dur.rstrip(')')
                            duration = dur
            tuitions = [
            {
                "study_mode": study_mode,
                "duration": duration,
                "student_category": "uk",
                "fee": uk_fee
            },
            {
                "study_mode":study_mode,
                "duration": duration,
                "student_category": "international",
                "fee": int_fee
            }
            ]
        except AttributeError:
            tuitions = []
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            dd_tag = soup.select('dd.paired-values-list__value')
            start_dates = []
            start_dates.extend(dd_tag[0].get_text(strip=True).split(','))
        except AttributeError:
            start_dates = []
        return start_dates

    # no application dates 'https://www.st-andrews.ac.uk/subjects/marine-biology/sustainable-aquaculture/' 'https://www.st-andrews.ac.uk/subjects/english/shakespeare-renaissance-literature-mlitt/'
    # explains all 4 empty lists
    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            application_dates = []
            headers = soup.find_all(re.compile('^h[1-6]$'), string=re.compile('application deadline', re.IGNORECASE))
            for header in headers:
                date_paragraph = header.find_next_sibling('p', string=re.compile(r'\w?\s?\d{1,2}[srnt]?[tdh]?\s\w+\s\d{4}'))
                # print(date_paragraph)
                if not date_paragraph:
                    dates = header.find_next_sibling('ul')
                    dates = dates.find_all('li')
                    for date in dates:
                        application_dates.append(date.text)
                if date_paragraph:
                    date_match = re.search(r'(\w?\s?\d{1,2}[srnt]?[tdh]?\s\w+\s\d{4})', date_paragraph.text)
                    if date_match:
                        application_dates.append(date_match.group(1))
            if application_dates == []:
                # print(soup.find_all('p'))
                dates = [p.text for p in soup.find_all('p') if p.text.lower().startswith('application deadline')]
                for date in dates:
                    date = date.split(':')[1]
                    application_dates.append(date)
        except AttributeError:
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements = None
            entry_requirements_heading = soup.find('h2', string='Entry requirements')
            entry_requirements_text = ''
            for sibling in entry_requirements_heading.find_next_siblings():
                if sibling.name == 'h3':
                    break
                entry_requirements_text += sibling.get_text(strip=True) + '\n'
            entry_requirements = entry_requirements_text
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            english_language_requirements = []
            lang_link = soup.find('h3', string='English language proficiency').find_next('a')['href']
            level = lang_link.split('/')[-2]
            english_language_requirements = self.lang_mappings[level]
        except (AttributeError, KeyError):
            english_language_requirements = []
            english_language_requirements = self.lang_mappings['7-d']
        return english_language_requirements
    def _get_individual_module(self, url, type):
        module = {}
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "lxml")
            first_link = soup.find('a', class_='search-result__link')['href']
            first_title = soup.find('h2', class_='search-result__heading').text.strip()
            module = {
                'title': first_title,
                'type': type,
                'link': first_link
            }
        except:
            module = {}
        return module
    def _get_modules(self, qualification, qualifications, course_title, soup: BeautifulSoup) -> List[dict]:
        try:
            modules = []
            qualification = qualification.strip()
            tabs = soup.find_all('li', role='presentation')
            content = soup.find_all('div', class_='tab-pane')
            mapping = {}
            for i in range(len(tabs)):
                tab_text = tabs[i].text.strip()
                if 'format' in tab_text:
                    break
                if 'undergraduate' in tab_text.lower():
                    continue
                # This part is convoluted because https://www.st-andrews.ac.uk/subjects/medicine/health-professions-education/ tab texts have no structure to them.
                flag = False
                if qualification not in tab_text: # yet another outlier (class of) https://www.st-andrews.ac.uk/subjects/art-history/digital-art-history-online/
                    for q in qualifications:
                        if q.strip() != qualification:
                            if q.strip() in tab_text and qualification not in tab_text: # another outlier (class of) https://www.st-andrews.ac.uk/subjects/computer-science/data-science/
                                flag = True
                                break
                if flag:
                    continue
                strong_texts = content[i].find_all('strong')
                if not strong_texts or 'Semester' in strong_texts[0].get_text():
                    strong_texts = content[i].find_all('li')
                if not strong_texts:
                    mapping[tab_text] = tab_text + course_title
                    continue
                for strong_text in strong_texts:
                    if len(strong_text.text) > 1:
                        mapping[strong_text.get_text()] = tab_text
            for title in mapping.keys():
                query = title.replace(' ', '%20')
                url = f"https://www.st-andrews.ac.uk/subjects/modules/search/?collection=uosa-module-catalogue-v2&profile=_default&form=simple&start_rank=1&num.ranks=10&mod=false&wildcard=1&query={query}&f.Year%7CAllDocumentsFill=All%20modules&sort=relevance"
                module = self._get_individual_module(url, mapping[title])
                if module == {}:
                    module = {
                        'title': title,
                        'type': mapping[title],
                        'link': ''
                    }
                modules.append(module)
        except AttributeError:
            modules = []
        return modules

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'lxml', from_encoding='utf-8')
        link = response.url
        title = self._get_title(soup)
        study_level = self.study_level
        university = self.university
        locations = self._get_locations(soup)
        description = self._get_description(soup)
        about = self._get_about(soup)
        # https://www.st-andrews.ac.uk/subjects/english/shakespeare-renaissance-literature-mlitt/ no starting date
        start_dates = response.meta['start_date'].split(',')
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)
        qf = self._get_qualification(soup) # dependent on qualification
        if '/' in qf:
            qf = qf.split('/')
        else:
            qf = qf.split(',')
            if 'or' in qf[-1]:
                splt = qf[-1].split('or')
                qf.remove(qf[-1])
                qf.extend(splt)
        for qualification in qf:
            qualification = qualification.strip()
            tuitions = self._get_tuitions(qualification, response.meta['duration'], response.meta['study_mode'], soup) # dependent on qualifications
            modules = self._get_modules(qualification, qf, title, soup) #dependent on qualifications

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
    cp.crawl(StAndrewsSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
