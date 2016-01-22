import scrapy,MySQLdb
from scrapy.selector import Selector
from scrapy.spiders import Spider
from bs4 import BeautifulSoup
import sys,urllib2,pprint,re
from django.utils.encoding import smart_str, smart_unicode
from naukriJobCrawl.items import DbOperations


class NaukriSpider(scrapy.Spider):
	name = "naukriJobCrawl"
	allowed_domains = ["jobsearch.naukri.com/"]
	start_urls = ["http://jobsearch.naukri.com/amazon-jobs"]
	db = DbOperations()	
	sql = """SELECT comp.txtName 
			FROM tblinterviwer as inter 
			INNER JOIN tblcompany as comp ON comp.GUID = inter.GUID_Company
			WHERE inter.IsActive = '1' GROUP BY comp.GUID ORDER BY count(inter.GUID) DESC"""
	results = db.executeQuery(sql)		
	start_urls = []
	for compName in results:		
	# sys.exit(0) 
		company = db.cleanName(compName[0])
		start_urls.append("http://jobsearch.naukri.com/"+company+"-jobs")
		for i in xrange(2,6):		
			start_urls.append("http://jobsearch.naukri.com/"+company+"-jobs-"+str(i))
		
		
	def parse(self,response):
		db = DbOperations()
		url = response.url		
		url = url.replace("http://jobsearch.naukri.com/","").replace("-jobs","")		
		urlCompany = re.sub("-[0-9]","",url)
		pp = pprint.PrettyPrinter(indent=4)
		sql = """SELECT comp.GUID
			FROM tblinterviwer as inter 
			INNER JOIN tblcompany as comp ON comp.GUID = inter.GUID_Company
			WHERE inter.IsActive = '1' 
			AND comp.txtName LIKE '%"""+urlCompany+"""%'
			GROUP BY comp.GUID ORDER BY count(inter.GUID) DESC LIMIT 1"""
		result = db.executeQuery(sql)
		if result:
			fk_comp_id = result[0][0]				
		else:
			return
		sel = Selector(response)
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
				try:
					jobAttr['jobSnippet'] = db.cleanSpacesAndCharacters(elementParser.find("span",itemprop="description").getText())				
				except:
					try:
						jobAttr['jobSnippet'] = db.cleanSpacesAndCharacters(elementParser.find("div",class_="more").getText())
					except:
						continue
				jobUrl = elementParser.find("a").get("href")
				jobAttr['jobUrl'] = jobUrl
				try:
					jobAttr['source'] = db.cleanSpacesAndCharacters(elementParser.find("div",class_ = "rec_details").getText())
				except:
					jobAttr['source'] = jobAttr['companyName']
				jobDescriptionParser = BeautifulSoup(urllib2.urlopen(jobUrl).read())				
				try:		
					jobAttr['jobDescription'] = ' '.join(jobDescriptionParser.find("ul",itemprop="description").getText().replace("\t","").replace("\n","").split()).replace("'","")
				except:
					try:
						jobAttr['jobDescription'] = ' '.join(jobDescriptionParser.find("div",class_="f14 lh18 alignJ disc-li").getText().replace("\t","").replace("\n","").split()).replace("'","")
					except:						
						jobAttr['jobDescription'] = jobDescriptionParser.find("meta",{"property":"og:description"})
						jobAttr['jobDescription'] = db.cleanSpacesAndCharacters(jobAttr['jobDescription']['content'])						
				sql = """INSERT INTO naukri_jobs_3 SET
						 jobtitle = '"""+smart_str(jobAttr['title'])+"""',
						 snippet = '"""+smart_str(jobAttr['jobSnippet'])+"""',
						 location = '"""+smart_str(jobAttr['jobLocation'])+"""',
						 naukri_company_name = '"""+smart_str(jobAttr['companyName'])+"""',
						 fk_company_id = '"""+smart_str(fk_comp_id)+"""',
						 job_url = '"""+smart_str(jobAttr['jobUrl'])+"""',
						 full_description = '"""+smart_str(MySQLdb.escape_string(jobAttr['jobDescription']))+"""',
						 source = '"""+smart_str(jobAttr['source'])+"""'"""								
				db.executeQuery(sql)
			else:
				# print "hello"
				continue			