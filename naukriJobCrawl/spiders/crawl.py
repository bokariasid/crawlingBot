# -*- coding: utf-8 -*- clstrFrm
# fields to always be changed:qtc,qp,fpsubmiturl,companyId
import scrapy
from scrapy.http import FormRequest
from scrapy.selector import Selector
from scrapy.linkextractors import LinkExtractor
import sys , urllib2 , pprint , re
from naukriJobCrawl.items import DbOperations
from bs4 import BeautifulSoup
companyCombo = []
urlCounter = 1
urlCompany = ""
db = DbOperations()
class CrawlSpider(scrapy.Spider):
    name = "naukriFormSubmit"
    global companyCombo
    allowed_domains = ["naukri.com"]
    start_urls = []
    pp = pprint.PrettyPrinter(indent=4)
    db = DbOperations()
    sql = "SELECT * FROM naukri_ro_company_mapping"
    results = db.executeQuery(sql)
    for company in results:
        companyAttributes = {}
        companyAttributes['name'] = ' '.join(company[3].split())
        companyAttributes['na_id'] = ' '.join(company[2].split())
        companyAttributes['ro_id'] = company[1]
        companyUrl = "http://www.naukri.com/"+db.cleanName(' '.join(company[3].split()))+"-jobs"
        companyAttributes['url'] = companyUrl
        companyCombo.append(companyAttributes)
        start_urls.append(companyUrl)
        for i in range (2,35):
            start_urls.append(companyUrl+"-"+str(i))

    def parse(self, response):
        global companyCombo,urlCounter,urlCompany
        company = []
        for companyAttr in companyCombo:
            if(companyAttr['url'] in response.url):
                company = companyAttr
                break
        if(urlCompany != company['name']):
            urlCompany = company['name']
            urlCounter = 1

        if(urlCounter == 1):
            urlCounter = urlCounter + 1
            formData = {"qp":company['name'],"ql":"","qe":"","qm":"","qx":"","qi[]":"","qf[]":"","qr[]":"","qs":"f","qo":"","qjt[]":"","qk[]":"","qwdt":"","qsb_section":"home","qpremTag":"","qpremTagLabel":"","qwd[]":"","qcf[]":"","qci[]":"","qck[]":"","edu[]":"","qcug[]":"","qcpg[]":"","qctc[]":"","qco[]":"","qcjt[]":"","qcr[]":"","qcl[]":"","qrefresh":"","xt":"adv","qtc[]":company['na_id'],"fpsubmiturl":company['url'],"src":"cluster","px":"1"}
        else:
            urlCounter = urlCounter + 1
            formData = {"qp":company['name'],"ql":"","qe":"","qm":"","qx":"","qi[]":"","qf[]":"","qr[]":"","qs":"f","qo":"","qjt[]":"","qk[]":"","qwdt":"","qsb_section":"home","qpremTag":"","qpremTagLabel":"","qwd[]":"","qcf[]":"","qci[]":"","qck[]":"","edu[]":"","qcug[]":"","qcpg[]":"","qctc[]":"","qco[]":"","qcjt[]":"","qcr[]":"","qcl[]":"","qrefresh":"","xt":"adv","qtc[]":company['na_id'],"fpsubmiturl":company['url']}

        yield FormRequest.from_response(response,
                                        formname='clstrFrm',
                                        formdata=formData,
                                        method="POST",
                                        dont_filter=True,
                                        callback=self.parse1)
    def parse1(self, response):
        global companyCombo,urlCounter,urlCompany
        sel = Selector(response)
        db = DbOperations()
        for companyAttr in companyCombo:
            if(companyAttr['url'] in response.url):
                company = companyAttr
                break
        jobs = sel.xpath('//div[contains(@type,"tuple")]').extract()
        if len(jobs) > 0:
            for job in jobs:
                jobAttr = self.getJobAttributes(job)
                db.insertJob(jobAttr,company['ro_id'])

    def parse2(self, response):
        global companyCombo,urlCounter,urlCompany
        sel = Selector(response)
        jobs = sel.xpath('//div[contains(@type,"tuple")]').extract()
        db = DbOperations()
        for companyAttr in companyCombo:
            if(companyAttr['url'] in response.url):
                company = companyAttr
                break
            else:
                return
        db.insertJobList(jobs,company['ro_id'])
        try:
            jobNextUrl = sel.xpath('//div[contains(@class,"pagination")]/a/@href').extract()[1]
            request = scrapy.Request(jobNextUrl , callback=self.parseNextUrl)
            yield request
        except:
            print "Unexpected error:", sys.exc_info()[0]

    def parseNextUrl(self, response):
        global companyCombo,urlCounter,urlCompany
        for companyAttr in companyCombo:
            if(companyAttr['url'] in response.url):
                company = companyAttr
                break
            else:
                return
        formData = {"qp":company['name'],"ql":"","qe":"","qm":"","qx":"","qi[]":"","qf[]":"","qr[]":"","qs":"f","qo":"","qjt[]":"","qk[]":"","qwdt":"","qsb_section":"home","qpremTag":"","qpremTagLabel":"","qwd[]":"","qcf[]":"","qci[]":"","qck[]":"","edu[]":"","qcug[]":"","qcpg[]":"","qctc[]":"","qco[]":"","qcjt[]":"","qcr[]":"","qcl[]":"","qrefresh":"","xt":"adv","qtc[]":company['na_id'],"fpsubmiturl":company['url']}
        yield FormRequest.from_response(response,
                                        formname='clstrFrm',
                                        formdata=formData,
                                        method="POST",
                                        dont_filter=True,
                                        callback=self.parse2)
    def getJobAttributes(self,job):
        global db
        jobAttr = {}
        elementParser = BeautifulSoup(job)
        jobAttr['jobId'] = db.cleanSpacesAndCharacters(elementParser.find("div",type="tuple").get("id"))
        sql = """SELECT * FROM naukri_jobs_4 WHERE  naukri_job_id = '"""+str(jobAttr['jobId'])+"""'"""
        result = db.executeQuery(sql)
        if not result:
            jobAttr['companyName'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="hiringOrganization").getText())
            jobAttr['title'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="title").getText())
            jobAttr['jobLocation'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="jobLocation").getText())
            jobAttr['experience'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="experienceRequirements").getText())
            jobAttr['salary'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="baseSalary").getText())
            try:
                jobAttr['jobSnippet'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="description").getText())
            except:
                try:
                    jobAttr['jobSnippet'] = db.cleanSpacesAndCharacters(elementParser.find("div",class_="more").getText())
                except:
                    try:
                        jobAttr['jobSnippet'] = db.cleanSpacesAndCharacters(elementParser.find("div",{"class":"desc"}).getText())
                    except:
                        pass
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
                            indexOfKeyword = jobAttr['jobDescription'].index("Keywords")
                            if indexOfKeyword:
                                jobAttr['jobDescription'] = jobAttr['jobDescription'][indexOfKeyword:]
                        except:
                            try:
                                jobAttr['jobDescription'] = ' '.join(jobDescriptionParser.findAll("td",{"class":"detailJob"})[2].getText().replace("\t","").replace("\n","").split()).replace("'","")
                            except:
                                jobAttr['jobDescription'] = jobAttr['jobSnippet']
            except:
                return

            return jobAttr
