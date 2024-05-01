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


class StrathSpider(scrapy.Spider):
    name = "strath"
    university = "University of Strathclyde"
    study_level = "Graduate"
    start_urls = [
        "https://www.strath.ac.uk/courses/postgraduatetaught/?level=Postgraduate+taught%7CPostgraduate+taught"
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
    pattern_ielts = re.compile(r"IELTS\s\d+(\.\d+)?")
    pattern_toefl = re.compile(r"TOEFL\s\d+(\.\d+)?")
    pattern_date = re.compile(
        r"\b\d{1,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}\b"
    )
    scrolling_script = """
    const scrolls = 10
    let scrollCount = 0
    
    // scroll down and then wait for 0.5s
    const scrollInterval = setInterval(() => {
      window.scrollTo(0, document.body.scrollHeight)
      scrollCount++
    
      if (scrollCount === numScrolls) {
        clearInterval(scrollInterval)
      }
    }, 2000)
    """

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(StrathSpider, cls).from_crawler(crawler, *args, **kwargs)
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
                    playwright_page_methods=[
                        PageMethod("evaluate", self.scrolling_script),
                        PageMethod(
                            "wait_for_selector",
                            "#course-search-results-show > section:nth-child(238)",
                        ),
                    ],
                ),
            )

    async def parse(self, response: HtmlResponse):
        page = response.meta["playwright_page"]
        await page.close()
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        courses = soup.select("article a")
        for course in courses:
            link = course.get("href")
            if link:
                query = parse_qs(link)
                course_link = next(iter(query["url"]), None)
                if course_link:
                    yield scrapy.Request(
                        course_link,
                        callback=self.parse_course,
                        meta=dict(
                            # playwright=True,
                            # playwright_include_page=True,
                            errback=self.errback,
                            course_link=course_link,
                        ),
                    )

    async def parse_course(self, response: HtmlResponse):
        # page = response.meta["playwright_page"]
        # await page.close()
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        title = self._get_title(soup)
        description = self._get_description(soup)
        university_title = self.university
        start_dates = self._get_start_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)
        language_requirements = self._get_english_language_requirements(soup)
        locations = self._get_locations(soup)
        study_level = self.study_level
        about = self._get_about(soup)
        start_dates = self._get_start_dates(soup)
        modules = self._get_modules(soup)
        qualifications = self._get_qualification(soup)
        if qualifications.find("/") != -1:
            for qualification in qualifications.split("/"):
                tuitions = self._get_tuitions(soup, qualification)
                yield {
                    "title": title,
                    "link": response.meta["course_link"],
                    "study_level": study_level,
                    "qualification": qualification,
                    "university_title": university_title,
                    "locations": locations,
                    "description": description,
                    "about": about,
                    "application_dates": [],
                    "start_dates": start_dates,
                    "entry_requirements": entry_requirements,
                    "modules": modules,
                    "tuitions": tuitions,
                    "language_requirements": language_requirements,
                }
        elif qualifications.find(",") != -1:
            for qualification in qualifications.split(","):
                tuitions = self._get_tuitions(soup, qualification)
                yield {
                    "title": title,
                    "link": response.meta["course_link"],
                    "study_level": study_level,
                    "qualification": qualification,
                    "university_title": university_title,
                    "locations": locations,
                    "description": description,
                    "about": about,
                    "application_dates": [],
                    "start_dates": start_dates,
                    "entry_requirements": entry_requirements,
                    "modules": modules,
                    "tuitions": tuitions,
                    "language_requirements": language_requirements,
                }
        else:
            tuitions = self._get_tuitions(soup, qualifications)
            yield {
                "title": title,
                "link": response.meta["course_link"],
                "study_level": study_level,
                "qualification": qualifications,
                "university_title": university_title,
                "locations": locations,
                "description": description,
                "about": about,
                "application_dates": [],
                "start_dates": start_dates,
                "entry_requirements": entry_requirements,
                "modules": modules,
                "tuitions": tuitions,
                "language_requirements": language_requirements,
            }

    def _get_title(self, course: Tag):
        try:
            title = course.select_one("span.course-title").text
        except AttributeError:
            title = ""
        return title

    def _get_locations(self, course: Tag):
        try:
            locations = []
            location_selector = course.select_one(".fa-map-marker").find_next("p")
            strong_tag = location_selector.find("strong")
            if strong_tag:
                strong_tag.extract()
            location = location_selector.get_text().strip()
            locations.append({"value": location})
        except AttributeError:
            locations = []
        return locations

    def _get_qualification(self, course: Tag):
        try:
            return course.select_one("span.superscript").text
        except AttributeError:
            return ""

    def _get_about(self, course: Tag):
        try:
            about = course.select_one("#whythiscourse .column-inner").prettify().strip()
        except AttributeError:
            about = ""
        return about

    def _get_description(self, course: Tag):
        try:
            description = course.select_one("#whythiscourse .column-inner p").text
        except AttributeError:
            try:
                description = course.select_one("#whythiscourse .column-inner div").text    # one exception
            except AttributeError:   
                description = ""
        return description

    def _get_start_dates(self, soup: BeautifulSoup):
        try:
            start_dates = []
            date_selector = soup.select_one(".fa-calendar-check-o").find_next("div")
            for i in date_selector:
                month_pattern = r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\b"
                dates = re.findall(month_pattern, i.text.strip())
                for date in dates:
                    value = date
                    start_dates.append({"value": date})
        except (IndexError, AttributeError):
            start_dates = []
        return start_dates

    def _get_entry_requirements(self, course: Tag):
        try:
            entry_requirements = course.select_one(
                "#entryrequirements table"
            ).prettify()
        except AttributeError:
            entry_requirements = ""
        return entry_requirements

    def _get_modules(self, soup: BeautifulSoup):
        try:
            modules = []
            for i in soup.select("div.course-module-title p"):
                try:
                    type = i.findPrevious(
                        "div", class_="course-module-subheader"
                    ).prettify()
                    if type.lower().find("<h3>"):
                        if type.lower().find("compulsory") != -1:
                            type = "Compulsory"
                        elif type.lower().find("elective") != -1:
                            type = "Elective"
                        elif type.lower().find("choose") != -1:
                            type = "Elective"
                        else:
                            type = (
                                i.findPrevious("div", class_="course-module-subheader")
                                .findNext("p")
                                .get_text()
                                .strip()
                            )
                except AttributeError:
                    type = ""
                if type.lower().find("choose") != -1:
                    type = "Elective"
                elif type.lower().find("chose") != -1:
                    type = "Elective"
                elif type.lower().find("option") != -1:
                    type = "Elective"
                elif type.lower().find("elective") != -1:
                    type = "Elective"
                else:
                    type = "Compulsory"

                modules.append(
                    {"title": i.get_text().strip(), "type": type, "link": ""}
                )

        except AttributeError:
            return []
        return modules
    
    def _get_duration(self, soup: BeautifulSoup, parsed_qualification: str) -> list[dict]: 
        try:
            durations = []

            duration_text = soup.select_one("span.fa.fa-pencil-square-o").next_sibling.get_text().strip()

            pattern = r'(?:(?P<qualification>\w+(\s+with\s+field\s+dissertation)?)\s*:\s*)?(?P<duration>\d+)\s+months\s+(?P<study_mode>\w+-time)'

            matches = re.finditer(pattern, duration_text)

            for match in matches:
                qualification = match.group('qualification')
                if qualification is None:
                    qualification = ''
                duration = int(match.group('duration'))
                study_mode = match.group('study_mode')
                qualification_info = {'qualification': qualification, 'duration': duration, 'study_mode': study_mode}
                durations.append(qualification_info)
        except:
            durations = []
        return durations
        

    def _get_tuitions(self, soup: BeautifulSoup, parsed_qualification: str):
        try:
            tuitions=[]
            # try:
            #     duration_text=soup.select_one("span.fa.fa-pencil-square-o").next_sibling.get_text().strip()
            # except:
            #     duration_text = ''
            # # print(duration_text)
            # mode_pattern = r'(full[- ]?time|part[- ]?time)'
            # try:
            #     study_mode = re.search(mode_pattern, duration_text,re.IGNORECASE).group(0)
            # except:
            #     study_mode = "Full-time"
            # # print(study_mode)
            # # duration_pattern = r'(?:\b(one|two|three|four|five|six|seven|eight|nine|ten)\b|\d+)\s+(?:months?|years?)'
            # duration_pattern = r'(?:\b(one|two|three|four|five|six|seven|eight|nine|ten|\d+)\b)\s+(week|weeks|month|months|year|years)'
            # try:
            #     duration = re.search(duration_pattern, duration_text,re.IGNORECASE).group(0)
            # except:
            #     # duration = "1 Year"
            #     duration = ""

            #TODO process durations from here.
            durations = self._get_duration(soup=soup, parsed_qualification=parsed_qualification)

            fee_pattern = r'[£€]([\d,]+)'
            tuition_table = soup.select_one('div.tab-inner table')
            # print(tuition_table)
            try:
                for i in tuition_table.select('tr'):
                    # print(i)
                    th = i.select_one("th").text
                    if th:
                        fee_pattern = r'£([\d,]+)'
                        for keyword in ["scotland", "england", "international", "strathclyde", "home", "ipgce", "cohort", "internal"]:
                            if keyword in th.lower():
                                # print(keyword)
                                try:
                                    category = i.select_one("th").get_text().strip()
                                except AttributeError as e:
                                    category = ""

                                # print(category)
                                try:
                                    fee_selector = i.select_one("td")
                                    # print(fee_selector)
                                    try:
                                        degree_selector = i.select_one("strong").text.strip()
                                        multiple_flag=1
                                    except:
                                        multiple_flag=0
                                    try:
                                        degree_selector = i.select_one("h3").text.strip()
                                        multiple_flag=1
                                    except:
                                        multiple_flag=0
                                    try:
                                        degree_selector = i.select_one("h4").text.strip()
                                        multiple_flag=0
                                    except:
                                        multiple_flag=0
                                    try:
                                        list_selector=fee_selector.select("li")
                                        for li in list_selector:
                                            fee_pattern = r'[£€]([\d,]+)'
                                            try:
                                                fee = re.search(fee_pattern, li.text).group(0)
                                            except:
                                                fee = ""
                                            if(li.text.lower().find("part-time") != -1):
                                                study_mode="Part-time"
                                            elif(li.text.lower().find("full-time") != -1):
                                                study_mode="Full-time"
                                            if li.text.find(parsed_qualification.lower()) != -1:
                                                multiple_flag = 1
                                            if(multiple_flag==1 and fee!=""):
                                                if(degree_selector.lower().find(parsed_qualification.lower()) != -1 or degree_selector.lower().find("2023/24") != -1 or li.text.find(parsed_qualification.lower()) != -1):
                                                    tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration})
                                                    break
                                            else:
                                                if(fee!=""):
                                                    tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration})
                                    except:
                                        pass
                                    try:
                                        degree_selector = i.select_one("strong").text.strip()
                                        multiple_flag=1
                                    except:
                                        multiple_flag=0
                                    try:
                                        degree_selector = i.select_one("h3").text.strip()
                                        multiple_flag=1
                                    except:
                                        multiple_flag=0
                                    try:
                                        paragraph=fee_selector.select("p")
                                        for p in paragraph:
                                            # fee_pattern = r'£([\d,]+)'
                                            fee_pattern = r'[£€]([\d,]+)'
                                            try:
                                                fee = re.search(fee_pattern, p.text).group(0)
                                            except:
                                                fee = ""
                                            if(p.text.lower().find("part-time") != -1):
                                                study_mode="Part-time"
                                            elif(p.text.lower().find("full-time") != -1):
                                                study_mode="Full-time"
                                            if(multiple_flag==1 and fee!=""):
                                                if(degree_selector.lower().find(parsed_qualification.lower()) or degree_selector.lower().find("2023/24")):
                                                    tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration})
                                            else:
                                                if(fee!=""):
                                                    tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration})
                                    except:
                                        pass

                                except Exception as e:
                                    # print(e)
                                    pass
                                break
                        
                        if parsed_qualification.lower() in th.lower() or 'fee' in th.lower():
                            category = "All"
                            cnt = 0
                            try:
                                
                                li = i.select_one("td").select("li")
                                for j in li:
                                    try:
                                        fee = re.search(fee_pattern, j.text).group(0)
                                    except:
                                        pass
                                    if fee:
                                        cnt += 1
                                        tt = j.find_previous('strong')
                                        if tt:
                                            if 'study mode' not in tt.text.lower() and len(tt.text.strip()) > 0:
                                                category = tt.text.strip()
                                            if(category.lower().find("scotland") == -1 and category.lower().find("england") == -1 and category.lower().find("international") == -1 ):
                                                category="All"
                                            tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration})
                                        else:
                                            tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration})
                                if cnt == 0:
                                    fee = re.search(fee_pattern, i.select_one("td").text).group(0)
                                    if fee:
                                        tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration})
                                    
                            except:
                                pass
                            # try:
                            #     fee = re.search(fee_pattern, i.select_one("td").text).group(0)
                            #     tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration}) 
                            # except:
                            #     pass
                        elif 'full-time' in th.lower():
                            category = "All"
                            study_mode = "Full-time"
                            try:
                                fee = re.search(fee_pattern, i.select_one("td").text).group(0)
                                if fee:
                                    tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration})
                            except:
                                pass
                        
                        elif 'tuition' in th.lower():
                            fee_selector = i.select_one("td")
                            category="All"
                            try:
                                list=fee_selector.select("li")
                                for li in list:
                                    fee_pattern = r'£([\d,]+)'
                                    fee = re.search(fee_pattern, li.text).group(0)
                                    duration= "1 Year" #as the fees per year is shown in this case
                                    if(li.text.lower().find("part-time") != -1):
                                        study_mode="Part-time"
                                    elif(li.text.lower().find("full-time") != -1):
                                        study_mode="Full-time"
                                    tuitions.append({"student_category": category, "fee": fee, "study_mode": study_mode, "duration": duration})
                            except:
                                pass
            except:
                pass
        except AttributeError:
            return []
        return tuitions

    def _get_english_language_requirements(self, soup: BeautifulSoup):
        try:
            language_requirements = []
            language_requirements_table = soup.select(
                "#entryrequirements table  tbody th"
            )
            for i in language_requirements_table:
                if i.text.lower().find("english language requirements") != -1:
                    requirement = i.findNext("td").get_text().strip()
                    try:
                        if requirement.lower().find("ielts") != -1:
                            ielts = self.pattern_ielts.search(requirement).group(0)
                            language_requirements.append(
                                {"language": "English", "score": ielts, "test": "IELTS"}
                            )
                    except:
                        pass
                    try:
                        if requirement.lower().find("toefl") != -1:
                            toefl = self.pattern_toefl.search(requirement).group(0)
                            language_requirements.append(
                                {"language": "English", "score": toefl, "test": "TOEFL"}
                            )
                    except:
                        pass
            if language_requirements == []:
                language_requirements.append(
                    {
                        "language": "English",
                        "score": "6.5 overall (no individual band less than 5.5)",
                        "test": "IELTS",
                    }
                )
        except AttributeError:
            language_requirements.append(
                {
                    "language": "English",
                    "score": "6.5 overall (no individual band less than 5.5)",
                    "test": "IELTS",
                }
            )  # because standard requirement is discussed like that in https://www.strath.ac.uk/studywithus/englishlanguagerequirements/
        return language_requirements

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()


if __name__ == "__main__":
    cp = CrawlerProcess(get_project_settings())

    cp.crawl(StrathSpider)
    cp.start()
