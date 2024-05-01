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
import requests


class HarperSpider(scrapy.Spider):
    name = "harper"
    university = "Harper cdAdams University"
    study_level = "Graduate"
    start_urls = [
        "https://www.harper-adams.ac.uk/courses/courses.cfm?layout=100&q=&type=postgraduate&title=&area=&yoe=&cpd=&max=50&start=1"
    ]
    output_path = (
        os.path.join("..", "data", "courses", "output")
        if os.getcwd().endswith("spiders")
        else os.path.join("course_crawler", "data", "courses", "output")
    )

    # Overrides configuration values defined in course_crawler/settings.py
    custom_settings = {
        "FEED_URI": Path(
            f"{output_path}/{name}/"
            f"{name}_graduate_courses_{datetime.today().strftime('%Y-%m-%d')}.json"
        )
    }
    ielts_pattern = re.compile(r"IELTS.*?(\d+\.\d+|\d+)")
    toefl_pattern = re.compile(r"TOEFL.*?(\d+\.\d+|\d+)")
    application_dates_pattern = r"(\d{1,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4})"
    start_date_pattern = re.compile(
        r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b"
    )
    id_pattern = r"\d{6}"
    default_application_dates=[]
    default_language_requirements=[]

    qualifications_id = {"PgC": 7, "PgD": 8, "MSc": 9, "MRes": 10, "MProf": 11}

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(HarperSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        output_path = (
            os.path.join("..", "data", "courses", "output")
            if os.getcwd().endswith("spiders")
            else os.path.join("course_crawler", "data", "courses", "output")
        )
        Path(f"{output_path}/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        apllication_url='https://www.harper-adams.ac.uk/apply/how-to-apply/595/direct-applications/'
        yield scrapy.Request(
            apllication_url,
            callback=self.parse_default_application_dates,
            meta=dict(
                # playwright=True,
                # playwright_include_page=True,
                # errback=self.errback,
            ),
        )
        languagle_url='https://www.harper-adams.ac.uk/university-life/international/339/english-language-requirements/'
        yield scrapy.Request(
            languagle_url,
            callback=self.parse_default_langauge_requirements,
            meta=dict(
                # playwright=True,
                # playwright_include_page=True,
                # errback=self.errback,
            ),
        )

        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_course_list,
                meta=dict(
                    course_link=url,
                    # playwright=True,
                    # playwright_include_page=True,
                    # errback=self.errback,
                ),
            )

    def parse_default_application_dates(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        application_dates=[]
        selector=soup.select("main li")
        for i in selector:
            date_pattern=re.compile(r'\d{1,2}\s\w+\s\d{4}')
            date=date_pattern.search(i.text)
            if date:
                self.default_application_dates.append({"value":date.group(0)})

    def parse_default_langauge_requirements(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        languagle_requirements=[]
        selector=soup.select_one("#site-wrapper table")
        row=selector.find_all("tr")
        for r in row:
            test=r.find_all("td")[0].text.strip().replace(" *","")
            score=r.find_all("td")[1].text.strip()
            if test=="Qualification":
                continue
            else:
                self.default_language_requirements.append({"language":"English","test":test,"score":score})

    def parse_course_list(self, response: HtmlResponse):
        # page = response.meta["playwright_page"]
        # await page.close()
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        courses = soup.select("article")
        for course in courses:
            link = "https://www.harper-adams.ac.uk" + course.select_one("a").get("href")
            title = self._get_title(course)
            qualification = self._get_qualification(course)
            if link:
                # query=parse_qs(link)
                # course_link=next(iter(query["url"],None))
                course_link = link
                if course_link:
                    yield scrapy.Request(
                        course_link,
                        callback=self.parse_course,
                        meta=dict(
                            # playwright=True,
                            # playwright_include_page=True,
                            # errback=self.errback,
                            course_link=course_link,
                            title=title,
                            qualification=qualification,
                        ),
                    )

    def parse_course(self, response: HtmlResponse):
        # page = response.meta["playwright_page"]
        # await page.close()
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        description = self._get_description(soup)
        university_title = self.university
        locations = self._get_locations(soup)
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)
        degree=response.meta["qualification"]
        tuitions=self._get_tuitions(soup,f'{degree}')
        qualifications = [
            qualification.strip()
            for qualification in response.meta["qualification"].split("/")
        ]
        about = self._get_about(soup)
        start_dates = self._get_start_dates(soup)
        tabmenu = soup.select(".tabmenu")
        if tabmenu:
            id = response.meta["course_link"].split("/")[-3].strip()
            for qualification in qualifications:
                route = self.qualifications_id.get(qualification)
                if route:               
                    yield scrapy.Request(
                        f"https://www.harper-adams.ac.uk/shared/get-pg-route-modules.cfm?id={id}&year_of_entry=2024&route={route}",
                        callback=self._parse_modules,
                        meta=dict(
                            # playwright=True,
                            # playwright_include_page=True,
                            # errback=self.errback,
                            title=response.meta["title"],
                            link=response.meta["course_link"],
                            qualification=qualification,
                            university_title=university_title,
                            locations=locations,
                            description=description,
                            about=about,
                            application_dates=application_dates,
                            start_dates=start_dates,
                            entry_requirements=entry_requirements,
                            tuitions=self._get_tuitions(soup,qualification),
                            language_requirements=language_requirements,
                        ),
                    )

                    
        else:
            yield {
                "title": response.meta["title"],
                "link": response.meta["course_link"],
                "study_level": self.study_level,
                "qualification": qualifications[0],
                "university_title": university_title,
                "locations": locations,
                "description": description,
                "about": about,
                "application_dates": application_dates,
                "start_dates": start_dates,
                "entry_requirements": entry_requirements,
                "tuitions": tuitions,
                "language_requirements": language_requirements,
                "modules": []
            }
    

    def _get_title(self, course: Tag):
        try:
            title_element = course.select_one("span.headline")
            title = "".join(title_element.find_all(text=True, recursive=False)).strip()
        except:
            title = ""
        return title

    def _get_qualification(self, course: Tag):
        try:
            qualification = course.select_one("span.small").text.strip()
        except AttributeError:
            qualification = ""
        return qualification

    def _get_description(self, course: Tag):
        try:
            if (
                course.select_one("#overview p")
                .text.lower()
                .find("application deadline")
                != -1
            ):
                description = (
                    course.select_one("#overview p").findNextSibling("p").text.strip()
                )
            else:
                description = course.select_one("#overview p").text.strip()
        except AttributeError:
            description = ""
        return description

    def _get_about(self, course: Tag):
        try:
            about = course.select_one("#overview").prettify()
        except AttributeError:
            about = ""
        return about

    def _get_entry_requirements(self, course: Tag):
        entry_requirements = ""
        try:
            entry_requirements = course.select_one("#entry-requirements").prettify()
        except AttributeError:
            try:
                for details in course.select("#overview h2"):
                    if details.text.lower().find("entry requirements") != -1:
                        try:
                            entry_requirements = details.findNext("ul").prettify()
                        except:
                            entry_requirements = details.findNext("ol").prettify()
            except AttributeError:
                entry_requirements = ""
        return entry_requirements

    def _get_locations(self, course: Tag):
        try:
            locations = []
        except AttributeError:
            locations = []
        return locations

    def _get_start_dates(self, soup: BeautifulSoup):
        try:
            start_dates = []
            dates_data = soup.select("#key-course-info p")
            for dates in dates_data:
                if re.search(self.start_date_pattern, dates.text.strip()) is not None:
                    date = re.search(self.start_date_pattern, dates.text.strip()).group(
                        0
                    )
                    start_dates.append({"value": date})
        except (IndexError, AttributeError):
            start_dates = []
        return start_dates

    def _get_application_dates(self, soup: BeautifulSoup):
        try:
            application_dates = []
            application_text = soup.select_one("#overview").text.strip()
            if re.search(self.application_dates_pattern, application_text) is not None:
                date = re.search(self.application_dates_pattern, application_text).group(0)
                application_dates.append({"value": date})
            else:
                application_dates = self.default_application_dates
        except AttributeError:
            application_dates=self.default_application_dates
        return application_dates

    def _parse_modules(self, response: HtmlResponse):
        # page = response.meta["playwright_page"]
        # await page.close()
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        modules = []
        subjects = soup.select("li a")
        for subject in subjects:
            title = subject.text.strip()
            link = f"https://www.harper-adams.ac.uk/shared/get-module.cfm?id={subject.get('title')}"
            if subject.findPrevious("strong").text.lower().find("optional") != -1:
                type = "Optional"
            elif subject.findPrevious("strong").text.lower().find("compulsory") != -1:
                type = "Compulsory"
            else:
                type = "Compulsory"
            modules.append({"title": title, "link": link, "type": type})

        yield {
            "title": response.meta["title"],
            "link": response.meta["link"],
            "study_level": self.study_level,
            "qualification": response.meta["qualification"],
            "university_title": response.meta["university_title"],
            "locations": response.meta["locations"],
            "description": response.meta["description"],
            "about": response.meta["about"],
            "application_dates": response.meta["application_dates"],
            "start_dates": response.meta["start_dates"],
            "entry_requirements": response.meta["entry_requirements"],
            "modules": modules,
            "tuitions":response.meta["tuitions"],
            "language_requirements": response.meta["language_requirements"],
        }

    def _parse_modules2(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        modules = response.meta['modules']
        subjects = soup.select("li a")
        for subject in subjects:
            title = subject.text.strip()
            link = f"https://www.harper-adams.ac.uk/shared/get-module.cfm?id={subject.get('title')}"
            if subject.findPrevious("strong").text.lower().find("optional") != -1:
                type = "Optional"
            elif subject.findPrevious("strong").text.lower().find("compulsory") != -1:
                type = "Compulsory"
            else:
                type = "Compulsory"
            modules.append({"title": title, "link": link, "type": type})
        # return modules


    def _get_modules(self, soup: BeautifulSoup, pageurl: str):
        try:
            modules = []
            id_str = pageurl.split("/")[-3].strip()
            # id = re.search(r'\d+', id_str).group(0)
            # self.logger.info(f"ID: {id_str}")
            for route in range(1, 20):
                url = f"https://www.harper-adams.ac.uk/shared/get-pg-route-modules.cfm?id={id_str}&year_of_entry=2024&route={route}"
                try:
                    response = requests.get(url, timeout=1)
                    # self.logger.info(f"ID: {id_str} Route: {route} URL: {url}")
                    if response.status_code == 200:
                        soup = BeautifulSoup(
                            response.content, "html.parser", from_encoding="utf-8"
                        )
                        qualification = soup.find("h3").get_text().split(" ")[0]
                        self.logger.info(f"{qualification}: {route}")
                        subjects = soup.select("li a")
                        for subject in subjects:
                            title = subject.text.strip()
                            link = (
                                "https://www.harper-adams.ac.uk/shared/get-module.cfm?id="
                                + subject.get("title")
                            )
                            if (
                                subject.findPrevious("strong")
                                .text.lower()
                                .find("optional")
                                != -1
                            ):
                                type = "Optional"
                            elif (
                                subject.findPrevious("strong")
                                .text.lower()
                                .find("compulsory")
                                != -1
                            ):
                                type = "Compulsory"
                            else:
                                type = "Compulsory"
                            modules.append({"title": title, "link": link, "type": type})
                    route += 1
                except:
                    pass

        except AttributeError:
            return []
        return modules

    def _get_tuitions(self, soup: BeautifulSoup, criteria: str):
        tuitions = []
        try:
            # tuition details for postgraduate taught 2024/25 is available in https://cdn.harper-adams.ac.uk/document/ki/finance/Fees-and-Charges-202425.pdf
            if criteria.lower().find("msc") != -1:
                if criteria.lower().find("advanced veterinary nursing") != -1:
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                            "duration": "1 year",
                            "fee": "£13440",
                        }
                    )
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "Overseas",
                            "duration": "1 year",
                            "fee": "£17166",
                        }
                    )
                elif criteria.lower().find("veterinary physiotherapy") != -1:
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                            "duration": "1 year",
                            "fee": "£13440",
                        }
                    )
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "Overseas",
                            "duration": "1 year",
                            "fee": "£17166",
                        }
                    )
                else:
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                            "duration": "1 year",
                            "fee": "£11520",
                        }
                    )
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "Overseas",
                            "duration": "1 year",
                            "fee": "£19750",
                        }
                    )
            if criteria.lower().find("mprof") != -1:
                tuitions.append(
                    {
                        "study_mode": "full-time",
                        "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                        "duration": "1 year",
                        "fee": "£11520",
                    }
                )
                tuitions.append(
                    {
                        "study_mode": "full-time",
                        "student_category": "Overseas",
                        "duration": "1 year",
                        "fee": "£19750",
                    }
                )
            if criteria.lower().find("pgd") != -1:
                if criteria.lower().find("veterinary physiotherapy") != -1:
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                            "duration": "1 year",
                            "fee": "9600",
                        }
                    )
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "Overseas",
                            "duration": "1 year",
                            "fee": "13166",
                        }
                    )
                elif criteria.lower().find("advanced veterinary nursing") != -1:
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                            "duration": "1 year",
                            "fee": "£9600",
                        }
                    )
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "Overseas",
                            "duration": "1 year",
                            "fee": "£13166",
                        }
                    )
                else:
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                            "duration": "1 year",
                            "fee": "£7680",
                        }
                    )
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "Overseas",
                            "duration": "1 year",
                            "fee": "£15750",
                        }
                    )
            if criteria.lower().find("pgc") != -1:
                if criteria.lower().find("advanced veterinary nursing") != -1:
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                            "duration": "1 year",
                            "fee": "£4800",
                        }
                    )
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "Overseas",
                            "duration": "1 year",
                            "fee": "£6583",
                        }
                    )
                else:
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                            "duration": "1 year",
                            "fee": "£3840",
                        }
                    )
                    tuitions.append(
                        {
                            "study_mode": "full-time",
                            "student_category": "Overseas",
                            "duration": "1 year",
                            "fee": "£7875",
                        }
                    )
            if criteria.lower().find("mres") != -1:
                tuitions.append(
                    {
                        "study_mode": "full-time",
                        "student_category": "UK, Republic of Ireland,Isle of Man & Channel Isles",
                        "duration": "1 year",
                        "fee": "£11520",
                    }
                )
                tuitions.append(
                    {
                        "study_mode": "full-time",
                        "student_category": "Overseas",
                        "duration": "1 year",
                        "fee": "£19750",
                    }
                )
        except AttributeError:
            tuitions = []
        return tuitions

    def _get_english_language_requirements(self, soup: BeautifulSoup):
        try:
            language_requirements = []
            language_requirements=self.default_language_requirements
        except AttributeError:
            language_requirements = []
        return language_requirements

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        # await page.close()


if __name__ == "__main__":
    cp = CrawlerProcess(get_project_settings())

    cp.crawl(HarperSpider)
    cp.start()