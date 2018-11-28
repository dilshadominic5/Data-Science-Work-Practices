import scrapy
import requests
import pandas as pd
from items import Myspider
import bs4
from bs4 import BeautifulSoup

s=requests.Session()
r = s.get("http://hydro.imd.gov.in/hydrometweb/(S(gub04afzvwz5b545o0jl3yqb))/DistrictRaifall.aspx")

class SpidyViewStateSpider(scrapy.Spider):
    name = 'Finalaul'#name of the spider
    start_urls = ['http://hydro.imd.gov.in/hydrometweb/(S(gub04afzvwz5b545o0jl3yqb))/DistrictRaifall.aspx']#root urls for the later crawls
    download_delay = 1.5

    def parse(self, response):      #a method that will be called to handle the response downloaded for each of the requests made
     for listItems in response.css('select#listItems > option ::attr(value)').extract():   #constructing selectors
            yield scrapy.FormRequest(
                r.url,
                formdata={
                    'listItems': listItems,
                    '__VIEWSTATE': response.css('input#__VIEWSTATE::attr(value)').extract_first()
                },
                callback=self.parse_tags
            )
    def parse_tags(self, response):
     for DistrictDropDownList in response.css('select#DistrictDropDownList > option ::attr(value)').extract():
        yield scrapy.FormRequest.from_response(                                               #will get list items and view state from previous step(def parse)
            response=response,
            formdata={'DistrictDropDownList': DistrictDropDownList},
            callback=self.parse_results,
        )
    def parse_results(self, response):
      my_Item=Myspider()
      for item in response.xpath('//*[@id="listItems"]').extract():
         for item1 in response.xpath('//*[@id="DistrictDropDownList"]').extract():
             for item2 in response.xpath('//*[@id="GridId"]').extract():
              my_Item['States']=response.xpath('//*[@id="listItems"]').extract()#first column in the dataframe
              a={}
              a['State']=my_Item['States']
              b=str(a)
              c=b[b.index('selected'):len(b)]
              d=c[c.index('>')+1:c.index('<')]
              #my_Item['States']=d
              my_Item['Districts']=response.xpath('//*[@id="DistrictDropDownList"]').extract()
              w={}
              w['District']=my_Item['Districts']
              x=str(w)
              y=x[x.index('selected'):len(x)]
              z=y[y.index('>')+1:y.index('<')]
              #my_Item['Districts']=z
              my_Item['Data']=response.xpath('//*[@id="GridId"]').extract()
              p={}
              p['ParsedData']=my_Item['Data']
              q=str(p)
              q=q.replace("\\r\\n","")
              l5=pd.read_html(q,header=1,)
              df = pd.concat(l5)
              df_month={'JAN':'M1', 'FEB':'M2', 'MAR':'M3', 'APR':'M4', 'MAY':'M5', 'JUN':'M6', 'JUL':'M7','AUG':'M8', 'SEPT':'M9', 'OCT':'M10', 'NOV':'M11', 'DEC':'M12'}
              soup = BeautifulSoup(q)
              u=[]
              r=[]
              for head in soup.find_all('th'):
                 f1=head.get_text().replace(" ","")
                 f1=f1.split()
                 f1 = [ df_month.get(item,item) for item in f1 ]
                 if f1:
                  u.append(f1[0])
              for l in u:#filling the cells
               a1=l[:]
               r.append(l)
               r.append(a1)
              r.remove('YEAR')
              df.dropna(axis=1,how='all',inplace=True)
              df.fillna('',inplace=True)
              w=df.iloc[0].tolist()
              label=list(zip(r,w))
              df1=pd.DataFrame(df.values,columns=label)
              df1 = df1.iloc[1:].reset_index(drop=True)
              df1['State']=d
              df1['District']=z
              my_Item['Data']=df1

             #we have to get d and z from previous data (state & district)
              yield my_Item
           

              
             