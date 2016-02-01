import scrapy,MySQLdb
from scrapy.selector import Selector
from scrapy.spiders import Spider
from bs4 import BeautifulSoup
import sys,urllib2,pprint,re
from django.utils.encoding import smart_str, smart_unicode
from naukriJobCrawl.items import DbOperations
from naukriJobCrawl.items import NaukrijobcrawlItem
companyCombo = {}
class NaukriSpider(scrapy.Spider):
    name = "naukriJobCrawl"
    allowed_domains = ["jobsearch.naukri.com/"]
    db = DbOperations()
    sql = """SELECT comp.txtName,comp.GUID
            FROM tblinterviwer as inter
            INNER JOIN tblcompany as comp ON comp.GUID = inter.GUID_Company
            WHERE inter.IsActive = '1' GROUP BY comp.GUID
            HAVING COUNT(*) >= 10
            ORDER BY count(inter.GUID) DESC"""
    results = db.executeQuery(sql)
    start_urls = []
    global companyCombo
    for compName in results:
        company = db.cleanName(compName[0])
        companyCombo[compName[1]] = company
        start_urls.append("http://jobsearch.naukri.com/"+company+"-jobs")
        for i in xrange(2,6):
            start_urls.append("http://jobsearch.naukri.com/"+company+"-jobs-"+str(i))
    sql = """SELECT comp.txtName, comp.GUID,llk.department_name
            FROM tblinterviwer AS inter
            INNER JOIN tblcompany AS comp ON comp.GUID = inter.GUID_Company
            INNER JOIN lookup_links AS llk ON comp.GUID = llk.fk_company
            WHERE inter.IsActive =  '1'
            GROUP BY comp.GUID
            HAVING COUNT( inter.GUID ) < 10
            ORDER BY COUNT( inter.GUID ) DESC """
    results = db.executeQuery(sql)
    for compString in results:
        company = db.cleanName(compString[0])
        companyCombo[compString[1]] = companyCombo
        departmentString = '-'.join([ x.replace('"','').replace(' ','-').replace(")","").replace("(","").replace(",","") for x in compString[2].split(" or ")])
        start_urls.append("http://jobsearch.naukri.com/"+company+"-"+departmentString+"-jobs")

    def parse(self,response):
        db = DbOperations()
        url = response.url
        url = url.replace("http://jobsearch.naukri.com/","").replace("-jobs","")
        urlCompany = re.sub("-[0-9]","",url)
        pp = pprint.PrettyPrinter(indent=4)
        global companyCombo
        for comp_id in sorted(companyCombo.iterkeys()):
            if urlCompany in companyCombo[comp_id]:
                 fk_comp_id = comp_id
        sel = Selector(response)
        try:
            jobs = sel.xpath('//div[contains(@class,"row")]').extract()
            jobAttr = {}
            for i in range(1,51):
                elementParser = BeautifulSoup(jobs[i])
                try:
                    jobAttr['companyName'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="hiringOrganization").getText())
                except:
                    continue
                if(jobAttr['companyName'].lower().find(urlCompany.lower()) == 0):
                    jobAttr['title'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="title").getText())
                    jobAttr['jobLocation'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="jobLocation").getText())
                    jobAttr['experience'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="experienceRequirements").getText())
                    jobAttr['salary'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="baseSalary").getText())
                    sql = """SELECT *
                            FROM naukri_jobs_3 WHERE
                            fk_company_id = '"""+str(fk_comp_id)+"""'
                            AND jobtitle = '"""+jobAttr['title']+"""'
                            AND location = '"""+jobAttr['jobLocation']+"""'"""
                    result = db.executeQuery(sql)
                    if result:
                        return
                    try:
                        jobAttr['jobSnippet'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="description").getText())
                    except:
                        try:
                            jobAttr['jobSnippet'] = db.cleanSpacesAndCharacters(elementParser.find("div",class_="more").getText())
                        except:
                            continue
                    try:
                        jobAttr['source'] = db.cleanSpacesAndCharacters(elementParser.find("div",class_ = "rec_details").getText())
                    except:
                        jobAttr['source'] = jobAttr['companyName']
                    jobUrl = elementParser.find("a").get("href")
                    jobAttr['jobUrl'] = jobUrl
                    try:
                        jobPage = urllib2.urlopen(jobUrl).read()
                        jobDescriptionParser = BeautifulSoup(jobPage)
                        try:
                            jobAttr['jobDescription'] = ' '.join(jobDescriptionParser.find("ul",itemprop="description").getText().replace("\t","").replace("\n","").split()).replace("'","")
                        except:
                            try:
                                jobAttr['jobDescription'] = ' '.join(jobDescriptionParser.find("div",class_="f14 lh18 alignJ disc-li").getText().replace("\t","").replace("\n","").split()).replace("'","")
                            except:
                                try:
                                    jobAttr['jobDescription'] = jobDescriptionParser.find("meta",{"property":"og:description"})
                                    jobAttr['jobDescription'] = db.cleanSpacesAndCharacters(jobAttr['jobDescription']['content'])
                                except:
                                    try:
                                        jobAttr['jobDescription'] = ' '.join(jobDescriptionParser.findAll("td",{"class":"detailJob"})[2].getText().replace("\t","").replace("\n","").split()).replace("'","")
                                    except:
                                        jobAttr['jobDescription'] = jobAttr['jobSnippet']
                    except:
                        print "hello"

                    sql = """INSERT INTO naukri_jobs_4 SET
                             jobtitle = '"""+jobAttr['title']+"""',
                             snippet = '"""+jobAttr['jobSnippet']+"""',
                             location = '"""+jobAttr['jobLocation']+"""',
                             naukri_company_name = '"""+jobAttr['companyName']+"""',
                             fk_company_id = '"""+str(fk_comp_id)+"""',
                             job_url = '"""+smart_str(jobAttr['jobUrl'])+"""',
                             experience = '"""+smart_str(jobAttr['experience'])+"""',
                             salary = '"""+smart_str(jobAttr['salary'])+"""',
                             full_description = '"""+smart_str(MySQLdb.escape_string(jobAttr['jobDescription']))+"""',
                             source = '"""+jobAttr['source']+"""'"""
                    db.executeQuery(sql)
                else:
                    # print "hello"
                    continue

        except:
            return