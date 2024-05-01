from datetime import datetime
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

class SwanseaSpider(scrapy.Spider):
    name = "swansea"
    university="Swansea University"
    study_level = "Graduate"
    start_urls = [
        'https://www.swansea.ac.uk/postgraduate/taught/'
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
    ielts_pattern=re.compile(r"IELTS.*?(\d+\.\d+|\d+)")
    toefl_pattern=re.compile(r"TOEFL.*?(\d+\.\d+|\d+)")
    application_dates_pattern=re.compile(r"(\d{1,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4})")
    start_date_pattern=re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|Ion|Chw|Maw|Ebr|Mai|Meh|Gor|Aws|Med|Hyd|Tac|Rha|)\s+\d{4}\b")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SwanseaSpider, cls).from_crawler(crawler, *args, **kwargs)
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
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta=dict(
                    course_link=url,
                    playwright=True,
                    playwright_include_page=True,
                    errback=self.errback,
                ),
            )

    async def parse(self, response: HtmlResponse):
        page = response.meta["playwright_page"]
        await page.close()
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        courses=soup.select("#complete-a-z li a")
        for course in courses:
            link=course.get("href")
            title=self._get_title(course)
            qualification=self._get_qualification(course)
            if link:
                # query=parse_qs(link)
                # course_link=next(iter(query["url"],None))
                course_link=link
                if course_link:
                    yield scrapy.Request(
                        course_link,
                        callback=self.parse_course,
                        meta=dict(
                            playwright=True,
                            playwright_include_page=True,
                            errback=self.errback,
                            course_link=course_link,
                            title=title,
                            qualification=qualification,
                        )
                    )
    async def parse_course(self, response: HtmlResponse):
        page = response.meta["playwright_page"]
        await page.close()
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        description = self._get_description(soup)
        university_title = self.university
        locations=self._get_locations(soup)
        start_dates=self._get_start_dates(soup)
        application_dates=self._get_application_dates(soup)
        entry_requirements=self._get_entry_requirements(soup)
        language_requirements=self._get_english_language_requirements(soup)
        study_level= self.study_level
        about=self._get_about(soup)
        start_dates=self._get_start_dates(soup)
        modules=self._get_modules(soup)
        tuitions=self._get_tuitions(soup)
        yield {
            "title": response.meta["title"],
            "link": response.meta["course_link"],
            "study_level": study_level,
            "qualification": response.meta["qualification"],
            "university_title": university_title,
            "locations": locations,
            "description": description,
            "about": about,
            "application_dates": application_dates,
            "start_dates": start_dates,
            "entry_requirements": entry_requirements,
            "modules":modules,
            "tuitions":tuitions,
            "language_requirements":language_requirements,

        }

    
    def _get_title(self, course: Tag):
        try:
            title= course.text.split(",")[0].replace("\n","").strip()
        except:
            title=""
        return title
    
    def _get_qualification(self, course: Tag):
        try:
            qualification=course.text.split(",")[1].strip()
        except AttributeError:
            qualification=""
        return qualification
    
    def _get_description(self, course: Tag):
        try:
            description= course.select_one(".col-sm-12.featured-course-content-content-pods p").text.strip()
        except AttributeError:
            description=""
        return description
    
        
    def _get_about(self, course: Tag):
        try:
            about= course.select_one(".col-sm-12.featured-course-content-content-pods").prettify()
        except AttributeError:
            about=""
        return about


    def _get_entry_requirements(self, course: Tag):
        try:
            entry_requirements= course.select_one("#entry-requirements").prettify()
        except AttributeError:
            try:
                entry_requirements= course.select_one("div #gofynion-mynediad-contents").prettify()
            except AttributeError:
                entry_requirements=""
        return entry_requirements
    

    
    def _get_locations(self, course: Tag):
        try:
            locations=[]
            location_table=course.select(".tab-content dl dt")
            for values in location_table:
                if(values.text.strip().lower().find("location")!=-1 or values.text.strip().lower().find("lleoliad")!=-1):
                    value=values.findNext("dd").text.strip()
                    locations.append({"value":value})
                    break
        except AttributeError:
            locations = []
        return locations
        
    
    
    def _get_start_dates(self, soup: BeautifulSoup):
        try:
            start_dates = []
            dates_data=soup.select(".featured-course-content-key-details td")
            for dates in dates_data:
                if(re.search(self.start_date_pattern, dates.text.strip()) is not None):
                    date=re.search(self.start_date_pattern, dates.text.strip()).group(0)
                    start_dates.append({"value":date})      
        except (IndexError, AttributeError):
             start_dates= []
        return start_dates
    
    def _get_application_dates(self, soup: BeautifulSoup):
        try:
            application_dates = []
            application_text=soup.select_one("#application-deadlines").text
            if(re.search(self.application_dates_pattern, application_text) is not None):
                date = re.search(self.application_dates_pattern, application_text).group(0)
                application_dates.append({"value":date})
            else:
                application_dates.append({"value":"22 September 2023"})
                application_dates.append({"value":"31 July 2023"})
                application_dates.append({"value":"15 December 2023"})
                application_dates.append({"value":"31 October 2023"}) #as standard application dates for postgraduate taught courses are described in https://www.swansea.ac.uk/admissions/application-deadlines/
        except AttributeError:
                application_dates.append({"value":"22 September 2023"})
                application_dates.append({"value":"31 July 2023"})
                application_dates.append({"value":"15 December 2023"})
                application_dates.append({"value":"31 October 2023"}) #as standard application dates for postgraduate taught courses are described in https://www.swansea.ac.uk/admissions/application-deadlines/
        return application_dates
    
    
    
    
        
    def _get_modules(self, soup: BeautifulSoup):
        try:
            modules=[]
            subjects=soup.select(".ppsm-ms-moduleTitle a")
            for subject in subjects:
                title=subject.text.strip()
                link=subject.get("href")
                if(subject.findPrevious("h5").text.lower().find("optional")!=-1):
                    type="Optional"
                elif(subject.findPrevious("h5").text.lower().find("compulsory")!=-1):
                    type="Compulsory"
                elif(subject.findPrevious("h5").text.lower().find("core")!=-1):
                    type="Core"
                else:
                    type="Compulsory"
                if(title!=""):
                    modules.append({"title":title,"link":link,"type":type})
        except AttributeError:
            return []
        return modules
    

        
    def _get_tuitions(self, soup: BeautifulSoup):
        try:
            tuitions=[]
            tuition=soup.select("#accordion-uk .card")
            for tuition_uk in tuition:
                student_category="UK"
                data=tuition_uk.select_one(".card-header a").text.strip()
                mode_pattern = re.compile(r'\b(Full|Part|Llawn|Rhan)\s*(Time|Amser)\b')
                study_mode=mode_pattern.search(data).group(0)
                duration_pattern=year_pattern = re.compile(r'\b\d+(\.\d+)?\s*(Blwyddyn|Year|Years|Blynedd)\b')
                duration=duration_pattern.search(data).group(0)
                for fees in tuition_uk.select("td"):
                    if(fees.text.strip().find("£")!=-1):
                        fee=fees.text.strip()
                        tuitions.append({"student_category":student_category,"study_mode":study_mode,"duration":duration,"fee":fee})
            tuition_int=soup.select("#accordion-int .card")
            for tuition_int in tuition_int:
                student_category="International"
                data=tuition_int.select_one(".card-header a").text.strip()
                mode_pattern = re.compile(r'\b(Full|Part|Llawn|Rhan)\s*(Time|Amser)\b')
                study_mode=mode_pattern.search(data).group(0)
                duration_pattern=year_pattern = re.compile(r'\b\d+(\.\d+)?\s*(Blwyddyn|Year|Years|Blynedd)\b')
                duration=duration_pattern.search(data).group(0)
                for fees in tuition_int.select("td"):
                    if(fees.text.strip().find("£")!=-1):
                        fee=fees.text.strip()
                        tuitions.append({"student_category":student_category,"study_mode":study_mode,"duration":duration,"fee":fee})
        except AttributeError:
            tuitions = []
        return tuitions
    
    def _get_english_language_requirements(self, soup: BeautifulSoup):
        try:
            language_requirements = []
            language=soup.select_one("#entry-requirements").text
            if(re.search(self.ielts_pattern, language) is not None):
                score = re.search(self.ielts_pattern, language).group(1)
                language_requirements.append({"language":"English","test":"IELTS","score":score})
            if(re.search(self.toefl_pattern, language) is not None):
                score = re.search(self.toefl_pattern, language).group(1)
                language_requirements.append({"language":"English","test":"TOEFL","score":score})  
        except AttributeError:
            language_requirements = []
        return language_requirements
    
        

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()


if __name__ == "__main__":
    cp = CrawlerProcess(get_project_settings())

    cp.crawl(SwanseaSpider)
    cp.start()
