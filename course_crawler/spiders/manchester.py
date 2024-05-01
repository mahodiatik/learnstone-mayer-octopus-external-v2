"""
@Author: John Doe
@Date: 01.01.2023.
"""

import re
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple

from functional import seq
from bs4 import BeautifulSoup, Tag

import requests
import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class ManchesterSpider(scrapy.Spider):

    name = "manchester"
    timestamp = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")

    university = "The University of Manchester"
    study_level = "Graduate"

    # URLs and Paths
    _CATALOG_URL = "https://www.manchester.ac.uk/study/masters/courses/list/"
    _CATALOG_XML_PATH = "xml/"
    _CONTENT_PATH = "all-content/"
    _COURSE_PROFILE_PATH = "#course-profile/"

    start_urls = [
        _CATALOG_URL + _CATALOG_XML_PATH,
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ManchesterSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    def spider_opened(self):
        Path(f"../data/courses/output/{self.name}").mkdir(parents=True, exist_ok=True)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse_course_list)

    def parse_course_list(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, "lxml", from_encoding="utf-8")

        course_list = soup.select("li")
        for course in course_list:
            hlink = course.find("a")
            url = hlink["href"]
            title = hlink.contents[0]
            qualifications = self._get_qualification(course)
            # url = "11428/msc-data-science-mathematics/"
            # title = "meta_title"
            yield scrapy.Request(
                url=self._CATALOG_URL
                + url
                + self._CONTENT_PATH
                + self._COURSE_PROFILE_PATH,
                callback=self.parse_course,
                meta={
                    "title": title,
                    "link": self._CATALOG_URL + url,
                    "qualifications": qualifications
                },
            )

    # Not Used
    def _get_title(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            title = None
        except AttributeError:
            title = None
        return title

    # done
    def _get_qualification(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            qualification = soup.select_one("div.degree").text
        except AttributeError:
            qualification = None
        return qualification

    
    # no locations in the course pages
    def _get_locations(self, soup: BeautifulSoup) -> List[str]:
        try:
            locations = []
        except AttributeError:
            locations = []
        return locations

    # done
    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            # description = soup.find(attrs={"name": "description"})["content"]
            description_section = (
                seq(soup.select("h3"))
                .find(lambda x: "description" in x.text.lower())
                .find_next_sibling("div")
            )
            description = description_section.getText(strip=True)
        except (KeyError, AttributeError):
            try:
                description = soup.select_one(
                    "div.course-profile-intro-copy"
                ).text.strip()
            except:
                description = ""
        return description

    # done
    def _get_about(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            about_section_ids = ["course-details", "aims"]
            about_section = []
            for id in about_section_ids:
                section = soup.find(id=id)
                if section and section.find_next_sibling("div"):
                    about_section.append(str(section.find_next_sibling("div").div))
                about = " ".join(about_section)
            if about == "":
                about = soup.select_one("div.course-profile-intro-copy").prettify()
        except AttributeError:
            try:
                about = soup.select_one("div.course-profile-intro-copy").prettify()
            except:
                about = ""
        return about

    # done
    def _get_tuitions(self,soup: BeautifulSoup, qualification) -> list:
        try:
            tuitions = []
            study_modes = ["full-time", "part-time"]
            student_categories = ["uk", "international"]
            fees_section = soup.find(id="fees").findNextSibling("ul")
            duration = "1 year" # as tuition fees are for 1 year (Fees are given as per annum) declared in the course pages
            for fee_section in fees_section.select("li"):
                try:
                    mode_section = fee_section.strong.text
                    study_mode = seq(study_modes).find(
                        lambda x: x in mode_section.lower()
                    )
                    fee_items = [
                        line.nextSibling.text.strip()
                        for line in fee_section.select("br")
                    ]
                    for item in fee_items:
                        student_category = seq(student_categories).find(
                            lambda x: x in item.lower()
                        )
                        fee_match = re.search("£[0-9]+,*[0-9]*", item)
                        fee = fee_match.group() if fee_match else None
                        if fee:
                            tuitions.append(
                                {
                                    "study_mode": study_mode,
                                    "duration": duration,
                                    "student_category": student_category,
                                    "fee": fee,
                                }
                            )
                except AttributeError:
                    pass
                try:
                    if tuitions == []:
                        mode_section = fee_section.strong.text
                        study_mode = seq(study_modes).find(
                            lambda x: x in mode_section.lower()
                        )
                        tuition = fee_section.text
                        fee_pattern = re.compile(r"(\d+,\d+)")
                        student_type = ""
                        if fee_pattern.search(tuition):
                            fee = fee_pattern.search(tuition).group(1)
                        if "uk" in tuition.lower():
                            student_category = "UK"
                            tuitions.append(
                                {
                                    "student_category": student_category,
                                    "fee": fee,
                                    "duration": duration,
                                    "study_mode": mode,
                                }
                            )
                        if "international" in tuition.lower():
                            student_category = "International"
                            tuitions.append(
                                {
                                    "student_category": student_category,
                                    "fee": fee,
                                    "duration": duration,
                                    "study_mode": mode,
                                }
                            )
                except:
                    tuitions = []
            fees_section = soup.find(id="fees").findNextSibling("div")
            for fee_section in fees_section.select("p"):
                try:
                    text = fee_section.text
                    mode = seq(study_modes).find(
                        lambda x: x in text.lower()
                    )
                    if(mode!=None):
                        study_mode=mode
                    student_category = seq(student_categories).find(
                        lambda x: x in text.lower()
                    )
                    fee_match = re.search("£[0-9]+,*[0-9]*", text)
                    fee = fee_match.group() if fee_match else None
                    if fee:
                        tuitions.append(
                            {
                                "study_mode": study_mode,
                                "duration": duration,
                                "student_category": student_category,
                                "fee": fee,
                            }
                        )
                except AttributeError:
                    pass
                try:
                    if tuitions == []:
                        mode_section = fee_section.strong.text
                        study_mode = seq(study_modes).find(
                            lambda x: x in mode_section.lower()
                        )
                        tuition = fee_section.text
                        fee_pattern = re.compile(r"(\d+,\d+)")
                        student_type = ""
                        if fee_pattern.search(tuition):
                            fee = fee_pattern.search(tuition).group(1)
                        if "uk" in tuition.lower():
                            student_category = "UK"
                            tuitions.append(
                                {
                                    "student_category": student_category,
                                    "fee": fee,
                                    "duration": duration,
                                    "study_mode": mode,
                                }
                            )
                        if "international" in tuition.lower():
                            student_category = "International"
                            tuitions.append(
                                {
                                    "student_category": student_category,
                                    "fee": fee,
                                    "duration": duration,
                                    "study_mode": mode,
                                }
                            )
                except:
                    tuitions = []


            
        except AttributeError:
            try:
                duration = ""
                mode = ""
                duration_selector = soup.select("p.key-information-label")
                for i in duration_selector:
                    if "duration" in i.text.lower():
                        duration_text = i.find_next("p").text
                        duration_pattern = re.compile(
                            r"\d+\s*(?:months?|weeks?|years?)"
                        )
                        if duration_pattern.search(duration_text):
                            duration = duration_pattern.search(duration_text).group(0)
                        type_pattern = re.compile(r"(?:full-time|part-time)")
                        if type_pattern.search(duration_text):
                            mode = type_pattern.search(duration_text).group(0)

                selector = soup.select(
                    ".course-profile-key-information_fees-and-funding li"
                )
                for i in selector:
                    if qualification.lower() in i.text.lower():
                        tuition = i.text
                        fee_pattern = re.compile(r"(\d+,\d+)")
                        student_type = ""
                        if fee_pattern.search(tuition):
                            fee = fee_pattern.search(tuition).group(1)
                        if "uk" in i.text.lower():
                            student_category = "UK"
                            tuitions.append(
                                {
                                    "student_category": student_category,
                                    "fee": fee,
                                    "duration": duration,
                                    "study_mode": mode,
                                }
                            )
                        if "international" in i.text.lower():
                            student_category = "International"
                            tuitions.append(
                                {
                                    "student_category": student_category,
                                    "fee": fee,
                                    "duration": duration,
                                    "study_mode": mode,
                                }
                            )
                if(tuitions==[]):
                    fee=soup.select_one("li.course-header-details_fees span").text
                    duration_text=soup.select_one("li.course-header-details_duration span").text
                    duration_pattern = re.compile(r"\d+\s*(?:months?|weeks?|years?)")
                    if duration_pattern.search(duration_text):
                        duration = duration_pattern.search(duration_text).group(0)
                    if (duration_text.lower().find('part-time') != -1):
                        study_mode='part-time'
                    else:
                        study_mode='full-time'
                    tuitions.append(
                        {
                            "student_category": "All",
                            "fee": fee,
                            "duration": duration,
                            "study_mode": study_mode,
                        }
                    )
            except:
                tuitions = []
        return tuitions

    #  No start dates in course pages
    def _get_start_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            start_dates = []
        except AttributeError:
            start_dates = []
        return start_dates

    # done
    def _get_application_dates(self, soup: BeautifulSoup) -> List[str]:
        try:
            apply_section_ids = [
                "staged-admissions",
                "how-to-apply",
                "advice-to-applicants",
            ]
            howtoapply_section = ""
            for id in apply_section_ids:
                section = soup.find(id=id)
                if section:
                    howtoapply_section += (
                        section.find_next_sibling("div")
                        .getText(strip=True)
                        .replace("\xa0", " ")
                    )

            # find dates using regex
            # date_pattern = r"\d{1,2}[rdsth]{0,2}\s\w+\s+\d{4}"
            date_pattern = r"\d{1,2}[rdsth]{0,2}\s(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}"
            application_dates = re.findall(
                pattern=date_pattern, string=howtoapply_section
            )
            if len(application_dates) == 0:
                date = soup.select_one(
                    "div.course-header-bg  li.course-header-details.course-header-details_enrollment  span"
                ).text
                application_dates.append(date)

        except AttributeError:
            try:
                date = soup.select_one(
                    " div.course-header-bg  li.course-header-details.course-header-details_enrollment  span"
                ).text
                application_dates.append(date)
            except:
                application_dates = []
        return application_dates

    # done
    def _get_entry_requirements(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            entry_requirements = soup.find(
                "h3", id="academic-entry-qualification"
            ).next_sibling.next_sibling.getText(strip=True)
        except AttributeError:
            try:
                entry_requirements = soup.select_one(
                    "div.course-profile-key-information.course-profile-key-information_how-to-apply"
                ).text.strip()
            except:
                entry_requirements = ""
        return entry_requirements

    # TODO get langage requirement page link
    def _get_language_requirements_page_link(
        self, soup: BeautifulSoup
    ) -> Optional[str]:
        try:
            english_link_text = seq(soup.select("a")).find(
                lambda x: "language requirements" in x.text.lower()
            )
            lang_req_link = english_link_text["href"]
            print(lang_req_link)
            return lang_req_link
        except:
            return None


    def _get_language_requirements_from_link(self,link):
        soup = BeautifulSoup(requests.get(link).content, "html.parser")
        language_requirements = []
        table= soup.select_one("#main__content table")
        if table is None:
            table=soup.select_one("#content table") 
        rows = table.find_all('tr')
        for row in rows[1:]:
            columns = row.find_all('td')
            test = columns[0].text.strip().replace(" >>","")
            score = columns[1].text.strip()
            language_requirements.append({'language': 'English', 'test': test, 'score': score})
        return language_requirements
        

    # TODO handle language requirement pages
    def _get_english_language_requirements(self, soup: BeautifulSoup) -> List[dict]:
        english_language_requirements = []
        try:
            link=self._get_language_requirements_page_link(soup)
            if link:
                english_language_requirements=self._get_language_requirements_from_link(link)
                if(len(english_language_requirements)>0):
                    return english_language_requirements    
            tests = ["IELTS", "TOEFL", "Pearson", "CAE", "CPE"]
            english_requirement_section = soup.find(
                id="english-language"
            ).next_sibling.next_sibling
            requirements = english_requirement_section.select("li")
            for requirement in requirements:
                _score = requirement.text.strip()
                _test = seq(tests).find(lambda x: x.lower() in _score.lower())
                if _test:
                    english_language_requirements.append(
                        {
                            "language": "English",
                            "test": _test,
                            "score": _score,
                        }
                    )
            if len(english_language_requirements) == 0:
                english_requirement_section = soup.find(
                    id="english-language"
                ).next_sibling
            requirements = english_requirement_section.select("li")
            for requirement in requirements:
                _score = requirement.text.strip()
                _test = seq(tests).find(lambda x: x.lower() in _score.lower())
                if _test:
                    english_language_requirements.append(
                        {
                            "language": "English",
                            "test": _test,
                            "score": _score,
                        }
                    )

        except (AttributeError, KeyError):
            try:
                english_language_requirements = []
                tests = ["IELTS", "TOEFL", "Pearson", "CAE", "CPE"]
                selectors = soup.select("div.tabAccordionContainer  h4")
                for selector in selectors:
                    if selector.text.lower().strip() == "english language":
                        requirement_list = selector.next_sibling.next_sibling
                        requirements = requirement_list.select("li")
                        for requirement in requirements:
                            _score = requirement.text.strip()
                            _test = seq(tests).find(
                                lambda x: x.lower() in _score.lower()
                            )
                            if _test:
                                english_language_requirements.append(
                                    {
                                        "language": "English",
                                        "test": _test,
                                        "score": _score,
                                    }
                                )
                if len(english_language_requirements) == 0:
                    selectors = soup.select("div.tabAccordionContainer  h4")
                    for selector in selectors:
                        if selector.text.lower().strip() == "english language":
                            requirement_list = selector.next_sibling
                            requirements = requirement_list.select("li")
                            for requirement in requirements:
                                _score = requirement.text.strip()
                                _test = seq(tests).find(
                                    lambda x: x.lower() in _score.lower()
                                )
                                if _test:
                                    english_language_requirements.append(
                                        {
                                            "language": "English",
                                            "test": _test,
                                            "score": _score,
                                        }
                                    )
            except:
                english_language_requirements = []
        return english_language_requirements

    # done
    def _get_modules(self, soup: BeautifulSoup, link, qualification: str) -> List[dict]:
        try:
            modules = []
            modules_table = soup.find("table", class_="course-units").tbody
            modules_data = modules_table.select("tr")
            for d in modules_data:
                _module = d.select("td")
                _title = _module[0].text
                try:
                    _link = link + self._CONTENT_PATH + _module[0].a["href"]
                except TypeError:
                    _link = "N/A"
                try:
                    _type = _module[3].text
                except IndexError:
                    _type = "Mandatory"
                modules.append(
                    {
                        "type": _type,
                        "title": _title,
                        "link": _link,
                    }
                )
        except AttributeError:
            try:
                modulesselector = soup.select(".CourseUnits h3")
                for module in modulesselector:
                    titletext = module.text
                    if re.match(r"^\d+\.", titletext):
                        title = titletext.split(". ")[1].strip()
                    else:
                        title = titletext
                    if (
                        module.next_sibling.text.lower().find(qualification.lower())
                        != -1
                    ):
                        next = module.next_sibling
                    else:
                        next = module.next_sibling.next_sibling
                    if next.text.lower().find("optional") != -1:
                        type = "Optional"
                    else:
                        type = "Mandatory"
                    modules.append({"title": title, "type": type, "link": ""})
            except:
                modules = []
        return modules

    def parse_course(self, response: HtmlResponse):
        soup = BeautifulSoup(response.body, "html.parser", from_encoding="utf-8")
        link = response.meta["link"]  #
        title = response.meta["title"]  #
        study_level = self.study_level  #
        qualifications = response.meta["qualifications"]  #
        university = self.university  #
        locations = self._get_locations(soup)
        description = self._get_description(soup)  #
        about = self._get_about(soup)  #
        start_dates = self._get_start_dates(soup)
        application_dates = self._get_application_dates(soup)
        entry_requirements = self._get_entry_requirements(soup)  #
        language_requirements = self._get_english_language_requirements(soup)  #
        for qualification in qualifications.split("/"):
            modules = self._get_modules(soup, link, qualification)
            tuitions = self._get_tuitions(soup, qualification)
            yield {
                "link": link,
                "title": title,
                "study_level": study_level,
                "qualification": qualification,
                "university_title": university,
                "locations": locations,
                "description": description,
                "about": about,
                "tuitions": tuitions,
                "start_dates": start_dates,
                "application_dates": application_dates,
                "entry_requirements": entry_requirements,
                "language_requirements": language_requirements,
                "modules": modules,
            }


def run():
    cp = CrawlerProcess(get_project_settings())
    cp.crawl(ManchesterSpider)
    cp.start()


if __name__ == "__main__":
    project_dir = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
    sys.path.append(project_dir)

    run()