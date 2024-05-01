"""
@Author: Md. Mahodi Atik Shuvo
@Date: 13.05.2023.
"""


import os
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple

from functional import seq
from bs4 import BeautifulSoup, Tag
import time
import requests
import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings



class SheffieldSpider(scrapy.Spider):
    name = 'sheffield'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

    university = 'University of Sheffield'
    study_level = 'Graduate'

    f = open("out.txt", 'a')

    english_language_certificates = []

    ielts_map = {
        'Overall IELTS score of 6.5 with a minimum of 6.0 in each component': 'Standard',
        'Overall IELTS score of 7.0 with a minimum of 6.5 in each component': 'Good',
        'Overall IELTS score of 7.5 with a minimum of 7.0 in each component': 'Advanced',
        'Overall IELTS score of 8.0 with a minimum of 7.5 in each component': 'Proficiency'
    }

    # TODO: add university course catalogue to start_urls
    start_urls = [
        'https://www.sheffield.ac.uk/postgraduate/taught/courses/2024'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SheffieldSpider, cls).from_crawler(crawler, *args,
                                                          **kwargs)  # TODO: change spider name to match university
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        # yield scrapy.Request(
        #     url='https://www.sheffield.ac.uk/postgraduate/taught/courses/2023/translation-studies-ma-pg-certificate-pg-diploma',
        #     callback=self.parse_one_tuition)
        # self.parse_one_tuition('MGTT192')
        yield scrapy.Request(
            url='https://www.sheffield.ac.uk/postgraduate/english-language',
            callback=self.parse_english_requirements)
        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)
        pass


    def parse_english_requirements(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        certificates = []
        head_values = []

        table = soup.find("table")
        head = table.find('thead')
        body = table.find('tbody')

        head_elems = head.find_all('th')

        for elem in head_elems:
            head_values.append(elem.text.strip())

        body_values = body.find_all('tr')

        for value in body_values:
            idx = 0
            certificate = {}

            exam_name = value.find('th')
            if exam_name != None:
                exam_name = exam_name.text.strip()
                tds = value.find_all('td')

                certificate[head_elems[idx].text.strip()] = exam_name
                idx += 1
                for td in tds:
                    score = td.text.strip()
                    certificate[head_elems[idx].text.strip()] = score
                    idx += 1

            certificates.append(certificate)

        self.english_language_certificates = certificates

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        course_list = soup.select('a[href^="/postgraduate/taught/courses/2024"]')
        for url in course_list:
            yield scrapy.Request(url=f"https://www.sheffield.ac.uk{url['href']}",
                                 callback=self.parse_course,
                                 dont_filter=True)

    def _get_title(self, soup: Tag) -> Optional[str]:
        try:
            title = soup.find("h1", {"class": "pgtitle"})
            title = title.text.strip()
        except AttributeError:
            title = None
        return title

    def _get_qualification(self, soup: BeautifulSoup) -> List[str]:
        try:
            qualification = []
            div_element = soup.find("div", {"class": "course-award"})
            spans = div_element.select('span')

            for span in spans:
                qualification.append(span.text.strip())

        except AttributeError:
            qualification = []
        return qualification

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            locations = ["Western Bank"]
        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            div_element = soup.find("div", {"id": "course-descript"})
            p_tags = div_element.find_all("p")
            description = ""

            for p in p_tags:
                description += p.text
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            div_element = soup.find("div", {"class": "pg-course-introd"})
            about = div_element.text.strip()
        except AttributeError:
            about = None
        return about

    def _get_tuitions(self, soup: BeautifulSoup,parsed_qualification: str):
        if(parsed_qualification=='PG Diploma'):
            parsed_qualification=['Postgraduate Diploma','PGDip','PG Diploma','Diploma']
        elif(parsed_qualification=='PG Certificate'):
            parsed_qualification=['Postgraduate Certificate','PGCert','PG Certificate','Certificate']
        tuitions=[]
        try:
            div_element = soup.find('div', class_='course-pgt-fee-lookup')
            course_code = div_element['data-course-internal-code'] 
        except:
            course_code = ""
        url = f"https://ssd.dept.shef.ac.uk/fees/pgt/api/drupal-lookup.php?course={course_code}"
        try:
            response= requests.get(url)
            data =response.json()
        except:
            data={}
        try:
            home_fee= data['2024']['Home']
            int_fee= data['2024']['Overseas']
        except:
            try:
                home_fee= data['2023']['Home']
                int_fee= data['2023']['Overseas']
            except:
                home_fee= ""
                int_fee= ""
        

        try:
            duration_selector= soup.select("#duration li")
            text= duration_selector[0].text
            for li in duration_selector:
                for qualification in parsed_qualification:
                    if qualification in li.text:
                        try:
                            duration_pattern = re.compile(r'(?<=\b)\S+\s+(?=year|years|academic year|academic years|yrs|yr)')
                            duration= duration_pattern.search(li.text).group(0)
                            if(duration!='1'):
                                duration= f"{duration} Years"
                            else:
                                duration= f"{duration} Year"
                        except:
                            try:
                                duration_pattern = re.compile(r'(?<=\b)\S+\s+(?=month|months)')
                                duration= duration_pattern.search(li.text).group(0) #try  catch
                                if(duration!='1'):
                                    duration= f"{duration} Months"
                                else:
                                    duration= f"{duration} Month"
                            except:
                                duration_pattern= re.compile(r'(?<=\b)\S+\s+(?=week|weeks)')
                                duration= duration_pattern.search(li.text).group(0)
                                if(duration!='1'):
                                    duration= f"{duration} Weeks"
                                else:
                                    duration= f"{duration} Week"
                        if(li.text.find('part-time')!=-1):
                            study_mode= 'Part-time'
                        else:
                            study_mode= 'Full-time'
                        if(home_fee!=''):
                            tuitions.append({"student_category":"UK","fee":home_fee,"duration":duration,"study_mode":study_mode})
                            tuitions.append({"student_category":"International","fee":int_fee,"duration":duration,"study_mode":study_mode})
            if(len(tuitions)==0):
                duration_selector= soup.select("#duration li")
                for li in duration_selector:
                    try:
                        duration_pattern = re.compile(r'(?<=\b)\S+\s+(?=year|years|academic year|academic years|yrs|yr)')
                        duration= duration_pattern.search(li.text).group(0)
                        if(duration!='1'):
                            duration= f"{duration} Years"
                        else:
                            duration= f"{duration} Year"
                    except:
                        try:
                            duration_pattern = re.compile(r'(?<=\b)\S+\s+(?=month|months)')
                            duration= duration_pattern.search(li.text).group(0) #try  catch
                            if(duration!='1'):
                                duration= f"{duration} Months"
                            else:
                                duration= f"{duration} Month"
                        except:
                            duration_pattern= re.compile(r'(?<=\b)\S+\s+(?=week|weeks)')
                            duration= duration_pattern.search(li.text).group(0)
                            if(duration!='1'):
                                duration= f"{duration} Weeks"
                            else:
                                duration= f"{duration} Week"
                    if(li.text.find('part-time')!=-1):
                        study_mode= 'Part-time'
                    else:
                        study_mode= 'Full-time'
                    if(home_fee!=''):
                        tuitions.append({"student_category":"UK","fee":home_fee,"duration":duration,"study_mode":study_mode})
                        tuitions.append({"student_category":"International","fee":int_fee,"duration":duration,"study_mode":study_mode})
        except:
            duration_selector=soup.select("#duration p")
            for p in duration_selector:
                for qualification in parsed_qualification:
                    if qualification in p.text:
                        try:
                            duration_pattern = re.compile(r'(?<=\b)\S+\s+(?=year|years|academic year|academic years|yrs|yr)')
                            duration= duration_pattern.search(p.text).group(0)
                            if(duration!='1'):
                                duration= f"{duration} Years"
                            else:
                                duration= f"{duration} Year"
                        except:
                            try:
                                duration_pattern = re.compile(r'(?<=\b)\S+\s+(?=month|months)')
                                duration= duration_pattern.search(p.text).group(0) #try  catch
                                if(duration!='1'):
                                    duration= f"{duration} Months"
                                else:
                                    duration= f"{duration} Month"
                            except:
                                duration_pattern= re.compile(r'(?<=\b)\S+\s+(?=week|weeks)')
                                duration= duration_pattern.search(p.text).group(0)
                                if(duration!='1'):
                                    duration= f"{duration} Weeks"
                                else:
                                    duration= f"{duration} Week"
                        if(p.text.find('part-time')!=-1):
                            study_mode= 'Part-time'
                        else:
                            study_mode= 'Full-time'
                        if(home_fee!=''):
                            tuitions.append({"student_category":"UK","fee":home_fee,"duration":duration,"study_mode":study_mode})
                            tuitions.append({"student_category":"International","fee":int_fee,"duration":duration,"study_mode":study_mode})
            if(len(tuitions)==0):
                duration_selector=soup.select("#duration p")
                for p in duration_selector:
                    try:
                        duration_pattern = re.compile(r'(?<=\b)\S+\s+(?=year|years|academic year|academic years|yrs|yr)')
                        duration= duration_pattern.search(p.text).group(0)
                        if(duration!='1'):
                            duration= f"{duration} Years"
                        else:
                            duration= f"{duration} Year"
                    except:
                        try:
                            duration_pattern = re.compile(r'(?<=\b)\S+\s+(?=month|months)')
                            duration= duration_pattern.search(p.text).group(0) #try  catch
                            if(duration!='1'):
                                duration= f"{duration} Months"
                            else:
                                duration= f"{duration} Month"
                        except:
                            duration_pattern= re.compile(r'(?<=\b)\S+\s+(?=week|weeks)')
                            duration= duration_pattern.search(p.text).group(0)
                            if(duration!='1'):
                                duration= f"{duration} Weeks"
                            else:
                                duration= f"{duration} Week"
                    if(p.text.find('part-time')!=-1):
                        study_mode= 'Part-time'
                    else:
                        study_mode= 'Full-time'
                    if(home_fee!=''):
                        tuitions.append({"student_category":"UK","fee":home_fee,"duration":duration,"study_mode":study_mode})
                        tuitions.append({"student_category":"International","fee":int_fee,"duration":duration,"study_mode":study_mode})
        
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            year = soup.find('span', {'class': 'startyear'})
            month = soup.find('span', {'class': 'startmonth'})

            start_dates = []

            start_dates.append(month.text.strip() + ", " + year.text.strip().split(' ')[0])
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
            entry_requirements = ""
            div_element = soup.find("div", {"id": "entry-req"})
            p_tags = div_element.find_all("p")
            for p in p_tags:
                text = p.text
                if text == "We also consider a wide range of international qualifications:":
                    break
                entry_requirements += p.text
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            english_language_requirements = []
            div_element = soup.find("div", {"id": "entry-req"})
            p_tags = div_element.find_all("p")
            for p in p_tags:
                text = p.text
                tokens = text.split(',')
                if tokens[0] in self.ielts_map:
                    key = self.ielts_map[tokens[0]]

                    for certificate in self.english_language_certificates:
                        english_language_requirements.append({
                            'language': 'English',
                            'test': certificate['Test'],
                            'score': certificate[key]
                        })

        except (AttributeError, KeyError):
            english_language_requirements = []
        return english_language_requirements

    def _get_modules(self, soup: BeautifulSoup,parsed_qualification) -> List[dict]:
        try:
            modules = []

            tabs_element = soup.find('div', {'class': 'uos-tabs'})
            if tabs_element is not None:
                tablist = tabs_element.find('div', {'aria-label': 'Years'})

                buttons = tablist.find_all('button')
                for button in buttons:
                    button['aria-selected'] = True
                    div_element = tabs_element.find_all('div', {'id': button['aria-controls']})
                    for div in div_element:
                        courses = div.find_all('dt', {'class': 'standalone-accordion'})
                        button_text = button.text.strip()
                        for course in courses:
                            type_element = course.find_previous('p')
                            if "optional" in type_element.text.lower():
                                type = "Optional"
                            else:
                                type = "Core"
                            if(button_text.lower().find(parsed_qualification.lower())!=-1):
                                modules.append({
                                    "type": type,
                                    "title": course.text.strip(),
                                    "link":""
                                })
                            elif(button_text.lower().find("core")!=-1):
                                type="Core"
                                if(parsed_qualification.lower().find("pg certificate")!=-1):
                                    parsed_qualification="PGCert"
                                elif(parsed_qualification.lower().find("pg diploma")!=-1):
                                    parsed_qualification="PGDip"
                                if(parsed_qualification.lower() in type_element.text.lower()):
                                    modules.append({
                                        "type": type,
                                        "title": course.text.strip(),
                                        "link":""
                                    })
                            elif(button_text.lower().find("optional")!=-1):
                                type="Optional"
                                if(parsed_qualification.lower().find("pg certificate")!=-1):
                                    parsed_qualification="PGCert"
                                elif(parsed_qualification.lower().find("pg diploma")!=-1):
                                    parsed_qualification="PGDip"
                                if(parsed_qualification.lower() in type_element.text.lower()):
                                    modules.append({
                                        "type": type,
                                        "title": course.text.strip(),
                                        "link":""
                                    })
                            else:
                                modules.append({
                                    "type": type,
                                    "title": course.text.strip(),
                                    "link":""
                                })
                        if(len(modules)==0):
                            for course in courses:
                                if(button_text.lower().find("core")!=-1):
                                    type="Core"
                                    modules.append({
                                        "type": type,
                                        "title": course.text.strip(),
                                        "link":""
                                    })
                                elif(button_text.lower().find("optional")!=-1):
                                    type="Optional"
                                    modules.append({
                                        "type": type,
                                        "title": course.text.strip(),
                                        "link":""
                                    })
                                
                                    


            else:
                module_element = soup.find('div', {'id': 'modules'})
                elem = module_element.find('h3')
                _type = ""
                while True:
                    if elem.name == 'h3':
                        if elem.text.find("Core") != -1 or elem.text.find("Optional") != -1:
                            _type = elem.text.split(':')[0]

                    elif elem.name == 'h4':
                        text = elem.text.split(':')[0]
                        if _type != "":
                            _type = text

                    elif elem.name == 'ul':
                        lis = elem.find_all('li')

                        courses = []

                        for li in lis:
                            modules.append({
                                'type': _type,
                                'title': li.text,
                                'link':''
                            })
                    elem = elem.find_next()
                    if elem.name == 'section':
                        break
            if(len(modules)==0):
                tabs_element = soup.find('div', {'class': 'uos-tabs'})
                if tabs_element is not None:
                    tablist = tabs_element.find('div', {'aria-label': 'Years'})

                    buttons = tablist.find_all('button')
                    for button in buttons:
                        button['aria-selected'] = True
                        div_element = tabs_element.find_all('div', {'id': button['aria-controls']})
                        for div in div_element:
                            courses= div.find_all('li')
                            for course in courses:
                                modules.append({
                                    "type": "Core",
                                    "title": course.text.strip(),
                                    "link":""
                                })

        except AttributeError:
            modules = []
        return modules


    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')

        link = response.url
        title = self._get_title(soup)
        study_level = self.study_level
        qualifications = self._get_qualification(soup)
        university = self.university
        locations = self._get_locations(soup)
        description = self._get_description(soup)
        about = self._get_about(soup)
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)
        for qualification in qualifications:
            tuitions = self._get_tuitions(soup,qualification)
            modules = self._get_modules(soup,qualification)
            yield {
                'link': link,
                'title': title,
                'study_level': study_level,
                'qualification': qualification,
                'university_title': university,
                'locations': locations,
                'about': about,
                'description': description,
                'tuitions': tuitions,
                'start_dates': start_dates,
                'application_dates': application_dates,
                'entry_requirements': entry_requirements,
                'language_requirements': language_requirements,
                'modules': modules
            }


def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(SheffieldSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
