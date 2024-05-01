"""
@Author: John Doe
@Date: 01.01.2023.
"""

import json
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
class ArtsSpider(scrapy.Spider):

    # TODO: change spider name to match university
    name = 'arts'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
    
    university = 'University of the Arts London'
    study_level = 'Graduate'


    # TODO: add university course catalogue to start_urls
    start_urls = [
        r'https://search.arts.ac.uk/s/search.json?collection=ual-courses-meta-prod&num_ranks=150&start_rank=1&query=!nullquery&f.Course%20level|level=Postgraduate&sort=relevance&profile=_default'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ArtsSpider, cls).from_crawler(crawler, *args, **kwargs)  # TODO: change spider name to match university
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_course_list)

    def parse_course_list(self, response: HtmlResponse):
        # soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
         data = response.json()
         course_list = data['response']['resultPacket']['results']
         for course in course_list:
             yield scrapy.Request(url=course['liveUrl'],
                         callback=self.parse_course,
                         dont_filter=True,
                         meta=dict(
                             link=course['liveUrl'],  
                             title=course['title'],
                         )
                         )

    def _get_title(self, soup: Tag) -> Optional[str]:
        try:
            title = soup.select_one('h1.heading1').text.strip()
        except AttributeError:
            title = ""
        return title

    def _get_qualification(self, course: Tag) -> Optional[str]:
        try:
            if(course.select_one('h1.heading1').text.strip().find("Graduate Diploma")!=-1):
                qualification = "Graduate Diploma"
            elif(course.select_one('h1.heading1').text.strip().find("PG Cert")!=-1):
                qualification = "PG Cert"
            elif(course.select_one('h1.heading1').text.strip().find("M ARCH")!=-1):
                qualification = "M ARCH"
            else:
                qualification = course.select_one('h1.heading1').text.split()[0].strip()
        except AttributeError:
            qualification = None
        return qualification

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            locations = []
            places=soup.select(".course-info .college-name")
            for place in places:
                text=place.text.strip()
                locations.append(text)

        except AttributeError:
            locations = []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description = soup.select_one("#course-overview p").text.strip()
        except AttributeError:
            description = None
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about = soup.select_one("#course-overview").prettify()
        except AttributeError:
            about = None
        return about

    def _get_tuitions(self, soup: BeautifulSoup) -> list:
        try:
            tuitions = []
            selector=soup.select("#fees-and-funding h3")
            for i in selector:
                if(i.text.strip().lower() == "home fee"):
                    fee=i.findNext("p").text.strip()
                    student_category="England"
                    tuitions.append({"study_mode":"full-time","fee":fee,"student_category":student_category,"duration":"1 year"})
                    
                elif(i.text.strip().lower() == "international fee"):
                    fee=i.findNext("p").text.strip()
                    student_category="International"
                    tuitions.append({"study_mode":"full-time","fee":fee,"student_category":student_category,"duration":"1 year"})
        except AttributeError:
            tuitions = []
        return tuitions

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_dates = []
            dates=soup.select(".course-info .course-start")
            for date in dates:
                start_dates.append(date.text.strip())
        except AttributeError:
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            application_dates = []
            pattern_date = re.compile(r"\b\d{1,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}\b")
            dates=soup.select("#apply-now div.home-tab p")
            for date in dates:
                if pattern_date.search(date.text):
                    value=pattern_date.search(date.text).group(0)
                    application_dates.append(value)

        except AttributeError:
            application_dates = []
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements = soup.select_one("section#application-process").prettify()
        except AttributeError:
            entry_requirements = None
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            english_language_requirements = []
            ielts_pattern=re.compile(r"IELTS.*?(\d+\.\d+|\d+)")
            toefl_pattern=re.compile(r"TOEFL.*?(\d+\.\d+|\d+)")
            ok=soup.select("section#application-process h3")
            for i in ok:
                if(i.text.strip().lower() == "english language requirements"):
                    data=i.findNext("p").text.strip()
                    if(ielts_pattern.search(data)):
                        score=ielts_pattern.search(data).group(0)
                        english_language_requirements.append({"language":"English","test":"IELTS","score":score,})

                    if(toefl_pattern.search(data)):
                        score=toefl_pattern.search(data).group(0)
                        english_language_requirements.append({"language":"English","test":"TOEFL","score":score,})

        except (AttributeError, KeyError):
            english_language_requirements = []
        return english_language_requirements

    def _get_modules(self, soup: BeautifulSoup) -> List[dict]:
        try:
            modules = []
            #as module type is not defined in the course pages the compulsory is considered as default
            try:
                moduletype=soup.select("#course_structure article h3")
                for i in moduletype:
                    if(i.text.strip().lower().find("autumn")!= -1 or i.text.strip().lower().find("spring")!= -1 or i.text.strip().lower().find("summer")!= -1 or i.text.strip().lower().find("block")!=-1):
                        continue
                    title=i.text.strip()
                    if(title.lower().find("programme specification")!= -1 or title.lower().find("learning and teaching methods")!= -1 or title.lower().find("mode of study")!=-1 or title.lower().find("important note concerning academic")!=-1 or title.lower().find("collaborative projects")!=-1):
                        break    
                    else:
                        if(title!=""):
                            modules.append({"title":title,"type":"Compulsory","link":""})
            except:
                pass
    
            try:
                moduletype=soup.select("#course_structure article h4")
                for i in moduletype:
                    title=i.text.strip()
                    if(title.lower().find("credits")!= -1):
                        modules.append({"title":title,"type":"Compulsory","link":""})
            except:
                pass
            try:
                moduletype=soup.select("#course_structure article strong")
                for i in moduletype:
                    if(i.text.strip().lower().find("block")!=-1):
                        continue
                    if(i.text.strip().lower().find("important note concerning academic")!= -1):
                        break
                    title=i.text.strip()
                    if(title!=""):
                        modules.append({"title":title,"type":"Compulsory","link":""})
            except:
                pass
            try:
                moduletype=soup.select("#course_structure article b")
                for i in moduletype:
                    title=i.text.strip()
                    if(title.lower().find("credits")!= -1):
                        first=i.findPrevious("b").text.strip()
                        modules.append({"title":f'{first}{title}',"type":"Compulsory","link":""})
            except:
                pass
            try:
                moduletype=soup.select("#course_structure article br")
                for i in moduletype:
                    title=i.text.strip()
                    if(title.lower().find("credits")!= -1):
                        modules.append({"title":title,"type":"Compulsory","link":""})
            except:
                pass
            try:
                moduletype=soup.select("#course_structure article li")
                for i in moduletype:
                    title=i.text.strip()
                    if(title.lower().find("credits")!= -1):
                        modules.append({"title":title,"type":"Compulsory","link":""})
            except:
                pass
            try:
                if(modules==[]):
                    moduletexts=soup.select("#course_structure article p")
                    for i in moduletexts:
                        text=i.text.strip()
                        pattern = r'([A-Za-z\s]+) Unit \((\d+) Credits\)'
                        if(re.match(pattern, text)):
                            match = re.findall(pattern,text)
                            for m in match:
                                title = m[0].strip()
                                credits = m[1].strip()
                                modules.append({"title":f'{title}"({credits} Credits)"',"type":"Compulsory","link":""})
                    
            except:
                pass
            if(modules==[]):
                moduletext=soup.select_one("#course_structure article p").text.strip()
                pattern = r'([A-Za-z\s]+) \((\d+) credits\)'
                if(re.match(pattern, moduletext)):
                    match = re.findall(pattern,moduletext)
                    for m in match:
                        title = m[0].replace(" 'and","").strip()
                        credits = m[1].strip()
                        modules.append({"title":f'{title}"({credits} Credits)"',"type":"Compulsory","link":""})


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
    cp.crawl(ArtsSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()