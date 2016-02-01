# -*- coding: utf-8 -*- clstrFrm
# fields to always be changed:qtc,qp,fpsubmiturl,companyId
import scrapy
from scrapy.http import FormRequest
from scrapy.selector import Selector
from scrapy.linkextractors import LinkExtractor
import sys,urllib2,pprint,re
from naukriJobCrawl.items import DbOperations
companyCombo = []
urlCounter = 1
urlCompany = ""
class CrawlSpider(scrapy.Spider):
    name = "naukriFormSubmit"
    global companyCombo
    allowed_domains = ["jobsearch.naukri.com"]
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
        companyUrl = "http://jobsearch.naukri.com/"+db.cleanName(' '.join(company[3].split()))+"-jobs"
        companyAttributes['url'] = companyUrl
        companyCombo.append(companyAttributes)
        start_urls.append(companyUrl)
        for i in range (2,35):
            start_urls.append(companyUrl+"-"+str(i))

    def parse(self, response):
        global companyCombo,urlCounter,urlCompany
        for companyAttr in companyCombo:
            if(companyAttr['url'] in response.url):
                company = companyAttr
                break
        if(urlCompany != company['name']):
            urlCompany = company['name']
            urlCounter = 1

        if(urlCounter == 1):
            urlCounter = urlCounter + 1
            formData = {"qp":company['name'],"ql":"","qe":"","qm":"","qx":"","qi[]":"","qf[]":"","qr[]":"","qs":"p","qo":"","qjt[]":"","qk[]":"","qwdt":"","qsb_section":"home","qpremTag":"","qpremTagLabel":"","qwd[]":"","qcf[]":"","qci[]":"","qck[]":"","edu[]":"","qcug[]":"","qcpg[]":"","qctc[]":"","qco[]":"","qcjt[]":"","qcr[]":"","qcl[]":"","qrefresh":"","xt":"adv","qtc[]":company['na_id'],"fpsubmiturl":company['url'],"src":"cluster","px":"1"}
        else:
            urlCounter = urlCounter + 1
            formData = {"qp":company['name'],"ql":"","qe":"","qm":"","qx":"","qi[]":"","qf[]":"","qr[]":"","qs":"p","qo":"","qjt[]":"","qk[]":"","qwdt":"","qsb_section":"home","qpremTag":"","qpremTagLabel":"","qwd[]":"","qcf[]":"","qci[]":"","qck[]":"","edu[]":"","qcug[]":"","qcpg[]":"","qctc[]":"","qco[]":"","qcjt[]":"","qcr[]":"","qcl[]":"","qrefresh":"","xt":"adv","qtc[]":company['na_id'],"fpsubmiturl":company['url']}

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
            db.insertJobList(jobs,company['ro_id'])

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
        formData = {"qp":company['name'],"ql":"","qe":"","qm":"","qx":"","qi[]":"","qf[]":"","qr[]":"","qs":"p","qo":"","qjt[]":"","qk[]":"","qwdt":"","qsb_section":"home","qpremTag":"","qpremTagLabel":"","qwd[]":"","qcf[]":"","qci[]":"","qck[]":"","edu[]":"","qcug[]":"","qcpg[]":"","qctc[]":"","qco[]":"","qcjt[]":"","qcr[]":"","qcl[]":"","qrefresh":"","xt":"adv","qtc[]":company['na_id'],"fpsubmiturl":company['url']}
        yield FormRequest.from_response(response,
                                        formname='clstrFrm',
                                        formdata=formData,
                                        method="POST",
                                        dont_filter=True,
                                        callback=self.parse2)