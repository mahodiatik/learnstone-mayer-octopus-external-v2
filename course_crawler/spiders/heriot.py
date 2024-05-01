"""
@Author: Md. Mahodi Atik Shuvo
@Date: 15.03.24
"""

import re
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple
from typing import Dict, List

from functional import seq
from bs4 import BeautifulSoup, Tag
from urllib.parse import parse_qs

import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


# TODO: change spider name to match university
class ExampleSpider(scrapy.Spider):

    # TODO: change spider name to match university
    name = 'heriot'
    timestamp = datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

    ielts_pattern=re.compile(r"IELTS.*?(\d+\.\d+|\d+)")
    toefl_pattern=re.compile(r"TOEFL.*?(\d+\.\d+|\d+)")
    pattern_date = re.compile(
        r"\b\d{1,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}\b"
    )
    
    university = 'Heriot-Watt University'
    study_level = 'Graduate'

    # TODO: add university course catalogue to start_urls
    start_urls = [
        'https://search.hw.ac.uk/s/search.html?gscope1=uk%2Conline%7C&profile=programmes&f.Level%7Clevel=Postgraduate&collection=heriot-watt%7Esp-programmes'
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
            yield scrapy.Request(
            url,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                errback=self.errback,
            ),
        )

    async def parse(self, response: HtmlResponse):
        page = response.meta["playwright_page"]
        await page.close()
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        courses = soup.select("table .clickable")
        for course in courses:
            course_name = self._get_title(course)
            qualification = self._get_qualification(course)
            search_link = self._get_search_link(response, course)
            level = self.study_level
            delivery = self._get_study_mode(course)
            location = self._get_locations(course)

            if search_link:
                query = parse_qs(search_link)
                course_link = next(iter(query["url"]), None)
                if course_link:
                    yield scrapy.Request(
                        course_link,
                        callback=self.parse_course,
                        meta=dict(
                            playwright=True,
                            playwright_include_page=True,
                            errback=self.errback,
                            course_name=course_name,
                            course_link=course_link,
                            level=level,
                            delivery=delivery,
                            location=location,
                            qualification=qualification,
                        ),
                    )

        next_page = self._get_next_page(soup)

        if next_page:
            yield scrapy.Request(
                response.urljoin(next_page),
                meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                    errback=self.errback,
                ),
            )

    def _get_next_page(self, soup):
        try:
            return soup.select_one(
                ".hw_course-search__pagination-link.hw_course-search__pagination-link--next"
            ).get("href")
        except:
            return None

    def _get_title(self, soup: Tag) -> Optional[str]:
        try:
            title= (
                soup.select_one(".hw_course-search__subject a strong")
                .get_text()
                .strip()
            )
        except:
            title= ""
        return title

    def _get_qualification(self, course: Tag) -> Optional[str]:
        try:
            qualification= (
            course.select_one(".hw_course-search__subject a")
            .contents[-1]
            .get_text()
            .replace(">", "")
            .strip()
        )
        except AttributeError:
            qualification = None
        return qualification
    
    def _get_search_link(self, response: HtmlResponse, course: Tag):
        return response.urljoin(
            course.select_one(".hw_course-search__subject a").get("href")
        )
    
    def _get_study_mode(self, course: Tag) -> List[str]:
        try:
            study_mode= [
                delivery.strip()
                for delivery in course.select_one(".hw_course-search__delivery")
                .get_text(separator="\n")
                .strip()
                .split("\n")
                if delivery.strip() != ""
            ]
        except AttributeError:
            study_mode = []
        return study_mode

    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            locations=[]
            location= soup.select_one(".hw_course-search__location").get_text().strip()
            locations.append(location)
        except:
            location= []
        return locations

    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            description = soup.select_one("div.pb-5 p").get_text().strip()
        except:
            try:
                description = soup.select_one("section.course-overview.grid-page-body p").get_text().strip()
            except:
                description = ""
        return description

    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about= soup.select_one("div.pb-5").prettify().strip().replace("\n", "")
        except:
            try:
                about = soup.select_one("section.course-overview.grid-page-body").prettify().strip()
            except:
                about = ""
        return about

    def _get_tuitions(self, soup: BeautifulSoup, meta_data: Dict[str, str]) -> list:
        try:
            tuitions = []
            tuition_table = soup.select_one("table.hw-content-blocks__table")
            if tuition_table:
                study_modes = [
                    x.get_text().strip().split("\n")[0]
                    for x in tuition_table.select("thead tr th")
                    if x.get_text().strip() != "" and x.get_text().find("Status") == -1
                ]
                rows = tuition_table.select("tbody tr")
                for row in rows:
                    category = row.select_one("th").get_text().strip().split("\n")[0]
                    fees = [fee.get_text().strip() for fee in row.select("td")]
                    for study_mode, fee in zip(study_modes, fees):
                        tuitions.append(
                            {
                                "student_category": category,
                                "fee": fee,
                                "study_mode": study_mode,
                                "duration": meta_data["Duration"]
                                if "Duration" in meta_data and study_mode == "Full-time"
                                else "",
                            }
                        )
            return tuitions
        except:
            return []

    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_dates=[]
            start_date_selector = soup.select_one('dt:contains("Start date") + dd')
            for date in start_date_selector.text.strip().split(","):
                start_dates.append(date.strip())
                
            
        except:
            start_dates=[]
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            application_dates=[]
            overview = soup.select_one("#overview").get_text().strip()
            date_list=self.pattern_date.findall(overview)
            for date in date_list:
                application_dates.append(date)
        except:
            application_dates=[]
        return application_dates

    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            selector = soup.select_one("div#entry-requirements")
            entry_requirements = ""
            selector = soup.select("div#entry-requirements p")
            for i in selector:
                next=i.find_next("h3")
                if(next.text=="English language requirements"):
                    entry_requirements+=str(i)
                else:
                    break
        except:
            entry_requirements= ""
        return entry_requirements

    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        try:
            language_requirements = []
            selector = soup.select("#entry-requirements p")
            for l in selector:
                language = l.get_text()
                if(re.search(self.ielts_pattern, language) is not None):
                    score = re.search(self.ielts_pattern, language).group(1)
                    language_requirements.append({"language":"English","test":"IELTS","score":score})
                if(re.search(self.toefl_pattern, language) is not None):
                    score = re.search(self.toefl_pattern, language).group(1)
                    language_requirements.append({"language":"English","test":"TOEFL","score":score})
        except:
            language_requirements= []
        return language_requirements
                    

    def _get_modules(self, soup: BeautifulSoup) -> List[dict]:
        try:
            courses = []
            for course in soup.select(
                "#course-content .tab-switcher__tab .rte-container ul"
            ):
                type = course.find_previous(["h3", "h4", "h5", "h6", "p"])
                if type:
                    type = type.get_text().strip()
                    if type.lower().find("option") != -1:
                        type = "Optional"
                    elif type.lower().find("core") != -1:
                        type = "Core"
                    elif type.lower().find("compulsory") != -1:
                        type = "Compulsory"
                    elif type.lower().find("project") != -1:
                        type = "Project"
                    else:
                        type = "Mandatory"
                else:
                    type = "Mandatory"
                for li in course.select("li"):
                    if li.get_text().replace("\n", "").strip() != "":
                        courses.append(
                            {
                                "title": li.get_text()
                                .replace("\n", "")
                                .split(":")[0]
                                .strip(),
                                "type": type,
                                "link": "",
                            }
                        )
            return courses
        except Exception as e:
            return []

    async def parse_course(self, response: HtmlResponse):
        page = response.meta["playwright_page"]
        await page.close()
        soup = BeautifulSoup(response.body, 'html.parser', from_encoding='utf-8')
        meta_data=self._get_meta_data(soup)
        study_level = self.study_level
        university = self.university
        description = self._get_description(soup)
        about = self._get_about(soup)
        tuitions = self._get_tuitions(soup,meta_data)
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)
        modules = self._get_modules(soup)

        yield {
            'link': response.meta["course_link"],
            'title': response.meta["course_name"],
            'study_level': study_level,
            'qualification': response.meta["qualification"],
            'university_title': university,
            'locations': response.meta["location"],
            'description': description,
            'about': about,
            'tuitions': tuitions,
            'start_dates': start_dates,
            'application_dates': application_dates,
            'entry_requirements': entry_requirements,
            'language_requirements': language_requirements,
            'modules': modules
        }
    
    def _get_meta_data(self, soup: BeautifulSoup):
        meta_data = {}
        for dt, dd in zip(soup.select("dl dt"), soup.select("dl dd")):
            key = dt.get_text().strip()  # Extract text and remove extra spaces
            value = dd.get_text().strip()  # Extract text and remove extra spaces
            meta_data[key] = value
        return meta_data

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()



def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(ExampleSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()
