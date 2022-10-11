# -*- coding: utf-8 -*-
"""
Created on Sun May 15 16:02:40 2022
@author: layto
Web Scraper Program (1/2 programs)
"""
#from bs4 import BeautifulSoup#bs4 has better parsers but I could not gather full data de to requirement for user interactions clicking more or accepting cookies
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import time
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
#import modin.pandas as pd#https://www.kdnuggets.com/2019/11/speed-up-pandas-4x.html faster but hits memory errors after ~125 Restaurants 25000 reviews
#import ray#ray is meant to work
#ray.init()
import sys, os
global driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

#from bs4 import BeautifulSoup#couldn't use beautiful soup, i needed to accept cookies and click more, beautiful soup has a 6x quicker parser
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver


#Please pip.install selenium
def InitialURLscrape():
    RegionSearch='Bebington_Wirral_Merseyside_England'#must be 3 long, can see yours online via tripadvisor
    RegionWanted='_Merseyside_'#I want all Merseyside, reduce to _England_ if you want to broaden or change to your region equivalent
    TopLevelURLs=set()
    TopLevelURLs.add('https://www.tripadvisor.co.uk/RestaurantSearch-g2549600-a_geobroaden.true-'+RegionSearch+'.html#EATERY_LIST_CONTENT')
    
    try:
        NumberOfRestaurantsIWantToQuery=100
        RestaurantURLs=set()
        i=0
        while len(RestaurantURLs)<=(NumberOfRestaurantsIWantToQuery): 
          
            i=i+1
            url='https://www.tripadvisor.co.uk/RestaurantSearch-g2549600-oa'+str(i*30)+'-a_geobroaden.true-'+RegionSearch+'.html#EATERY_LIST_CONTENTS'#*30 as when increment by 10 wasting time as the list of o20 and 030 has an overlap of 20 Restaurants
            
            URLsx30=GetRestaurantURLs(url,RegionWanted)#returns sub list of 30 restaurants
            for x in URLsx30:
                if x[0]!=x and x not in RestaurantURLs:#set prevents duplicate URLs but different formats means caution is needed
                    RestaurantURLs.add(x)
        
    except Exception as e:
            print('Error scraping initial Page',i,e)
     
    RestaurantURLs=list(RestaurantURLs)
    RestaurantURLs=RestaurantURLs[:NumberOfRestaurantsIWantToQuery]#will grab up to a page more restaurants than actually wanted
    try:
        AttemptURLs={'URL':RestaurantURLs,
      
            }
        AttemptURLs=pd.DataFrame(AttemptURLs)
        AttemptURLs.to_csv('AttemptingRestaurantsURLs.csv', index=True,encoding='utf-8-sig')
    except Exception as e:
        print('error storing urls attempted to query',e)
 
    ALLRestaurants=pd.DataFrame()
    RestaurantReviewCount=[]
    RestaurantReviewActual=[]
    b=0
    StartTime=time.time()
    CalledRestaurantURLs=[]
    
    RatedExcellent=[]#RatingList[0]
    RatedVery_good=[]#RatingList[1]
    RatedAverage=[]#RatingList[2]
    RatedPoor=[]#RatingList[3]
    RatedTerrible=[]#RatingList[4]
    ListRestaurantName=[]
    ListRestaurantLocation=[]
    for i in RestaurantURLs:
        try:
            b=b+1
            print('Restaurant number',b,'of',len(RestaurantURLs))
            SingleRestaurant,NumberOfReviews,RatingList,RestaurantName,RestaurantLocation=FirstPageIntoAllReviewPages(i)#collects df of review level data then seperate restaurant level aggregated data
            SingleRestaurant=SingleRestaurant.drop_duplicates()
            ALLRestaurants=pd.concat([ALLRestaurants, SingleRestaurant], axis=0)
            lenpage=len(SingleRestaurant['Restaurant_Review_Text'])           
            print('Reviews scraped so far',len(ALLRestaurants['Restaurant_Review_Title']),len(ALLRestaurants['Restaurant_Review_Title'])/(-StartTime+time.time()),'Reviews per second')      
            print('Ran for',-StartTime+time.time())
            print(lenpage,'/ ',NumberOfReviews)
            try:
                RestaurantReviewActual.append(lenpage)
                CalledRestaurantURLs.append(i)
                RestaurantReviewCount.append(NumberOfReviews)
                ListRestaurantName.append(RestaurantName)
                ListRestaurantLocation.append(RestaurantLocation)
            except:
                a=0
            try:
                RatedTerrible.append(RatingList[4])#if only partial list would throw error here and keep lists same size
                RatedExcellent.append(RatingList[0])
                RatedVery_good.append(RatingList[1])
                RatedAverage.append(RatingList[2])
                RatedPoor.append(RatingList[3])
                               
            except: 
                RatedExcellent.append('')#if error list is same length as main, so can be used to make dataframe
                RatedVery_good.append('')
                RatedAverage.append('')
                RatedPoor.append('')
                RatedTerrible.append('')
            try:
               if b%50==0:#as program can be left idle for days I want way to see data as we go
                   ALLRestaurants.to_csv('ALLRestaurantsDataCatch'+str(b)+'.csv', index=True,encoding='utf-8-sig')
                   ALLURLs={'URL':CalledRestaurantURLs,
                            'RestaurantName':ListRestaurantName,
                            'RestaurantLocation':ListRestaurantLocation,
                            'MaxPossibleScrapedReviews':RestaurantReviewCount,
                            'RestaurantReviewActual':RestaurantReviewActual,
                            'RatedExcellent':RatedExcellent,#=[]#RatingList[0]
                            'RatedVery_good':RatedVery_good,#=[]#RatingList[1]
                            'RatedAverage':RatedAverage,#=[]#RatingList[2]
                            'RatedPoor':RatedPoor,#=[]#RatingList[3]
                            'RatedTerrible':RatedTerrible#=[]
                     
                   }
                   ALLURLs=pd.DataFrame(ALLURLs)
                   ALLURLs.to_csv('ALLRestaurantsURLsCatch'+str(len(CalledRestaurantURLs))+'.csv', index=True,encoding='utf-8-sig')                 
            except:
               a=0
        except Exception as e:
           # print('error passing url to run by restaurant',e)#in here to patch an error left as useful for future
          #  exc_type, exc_obj, exc_tb = sys.exc_info()
           # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
          #  print(exc_type, fname, exc_tb.tb_lineno)
            try:#after error tries to save data so far
               try:#if first file write fails still tries second
                   ALLRestaurants.to_csv('ALLRestaurantsDataCatch'+str(b)+'.csv', index=True,encoding='utf-8-sig')
               except:
                   a=0
               ALLURLs={'URL':CalledRestaurantURLs,
                        'RestaurantName':ListRestaurantName,
                            'RestaurantLocation':ListRestaurantLocation,
                            'MaxPossibleScrapedReviews':RestaurantReviewCount,
                            'RestaurantReviewActual':RestaurantReviewActual,
                            'RatedExcellent':RatedExcellent,#=[]#RatingList[0]
                            'RatedVery_good':RatedVery_good,#=[]#RatingList[1]
                            'RatedAverage':RatedAverage,#=[]#RatingList[2]
                            'RatedPoor':RatedPoor,#=[]#RatingList[3]
                            'RatedTerrible':RatedTerrible#=[]
                     
                   }
               ALLURLs=pd.DataFrame(ALLURLs)
               ALLURLs.to_csv('ALLRestaurantsURLsCatch'+str(len(CalledRestaurantURLs))+'.csv', index=True,encoding='utf-8-sig')                 
            except:
               a=0
    try:
        ALLURLs={'URL':CalledRestaurantURLs,
                 'RestaurantName':ListRestaurantName,
                 'RestaurantLocation':ListRestaurantLocation,
                 'MaxPossibleScrapedReviews':RestaurantReviewCount,
                 'RestaurantReviewActual':RestaurantReviewActual,
                 'RatedExcellent':RatedExcellent,#=[]#RatingList[0]
                 'RatedVery_good':RatedVery_good,#=[]#RatingList[1]
                 'RatedAverage':RatedAverage,#=[]#RatingList[2]
                 'RatedPoor':RatedPoor,#=[]#RatingList[3]
                 'RatedTerrible':RatedTerrible#=[]
            }
        ALLURLs=pd.DataFrame(ALLURLs)
        ALLURLs.to_csv('ALLRestaurantsURLs'+str(len(CalledRestaurantURLs))+'.csv', index=True,encoding='utf-8-sig')
    except:
        print('error storing urls attempted to query')
    try:   
        print(ALLRestaurants.head())
        print(ALLRestaurants.tail())
        #Data=Data.drop_duplicates(inplace=True)#no need to drop duplicates could be in future
        print(len(ALLRestaurants['Restaurant_Name']))
        ALLRestaurants.to_csv('ALLRestaurantsData'+str(len(CalledRestaurantURLs))+'.csv', index=True,encoding='utf-8-sig')
    except Exception as e:
        print('Error in writing Restaurant file',e)
def GetRestaurantURLs(url,RegionWanted):
    driver.get(url)
    try:
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="onetrust-accept-btn-handler"]' ))).click()#click accept cookies banner
    except:
       # print('Error accepting cookies')
       a=1
    num=0
    #find excessive links
    num=0
    elems = driver.find_elements_by_xpath("//a[@href]")
    URLs=set()
    for elem in elems:
        num=num+1
        elem=elem.get_attribute("href")    #cut down links to required
        temp=elem.find('ShowUserReviews')
        place=0
        if temp>10:
            elem2=elem
            totalplace=0
            for i in range(1,5):
                place=elem2.find('-')+1
                totalplace=totalplace+place
                elem2=elem2[place:]
            elem=elem[:totalplace]+'Reviews-'+elem[totalplace:]
            elem=elem.replace('ShowUserReviews','Restaurant_Review')
        if RegionWanted in elem and elem not in URLs and 'Restaurants' not in elem and 'Restaurant' in elem and '#' not in elem and '-or' not in elem and 'RegistrationController' not in elem and 'a_geobroaden' not in elem:# some URLs are not restaurants this helps to filter them out
            URLs.add(elem)   #restaurants is usually another page listing many restaurants, where as restaurant is
    return URLs


def FirstPageIntoAllReviewPages(masterurl):
    print('Firstpage into all reviews',masterurl)
    try:
        insertplace=masterurl.find('Reviews')+7
        RestaurantNameLocation=masterurl[insertplace+1:len(masterurl)-5]
        Split=RestaurantNameLocation.find('-')
        RestaurantLocation=RestaurantNameLocation[Split+1:]
        RestaurantName=RestaurantNameLocation[:Split]    
        RestaurantData=Read_A_Page_Reviews(masterurl,masterurl,RestaurantName,RestaurantLocation)#first page result seperate so rest can be concatted onto this DataFrame     
        driver.get(masterurl)
        try:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="onetrust-accept-btn-handler"]' ))).click()#click accept cookies banner
        except:
            a=1
        try:#rating of restaurant as a whole
            RatingList=[]
            for i in range(0,5):
                rating=str(driver.find_elements_by_class_name('row_num')[i].text)
                rating=rating.replace(',','')
                RatingList.append(rating)#make array
        except:
            RatingList=['','','','','']
        try:
            NumberOfReviews=0
            for i in RatingList:
                if i=='':
                    i=0
                NumberOfReviews=NumberOfReviews+int(i)
            if NumberOfReviews==0:
                try:
                    NumberOfReviews= driver.find_elements_by_class_name('reviews_header_count')[0].text#responses from owner included in this number
                    NumberOfReviews=NumberOfReviews.replace('(','')
                    NumberOfReviews=NumberOfReviews.replace(')','')
                except:
                    NumberOfReviews=0
            NumberOfPages=int(NumberOfReviews)/10
            if NumberOfPages%1!=0:
                NumberOfPages=int(NumberOfPages)+1
            else:
                NumberOfPages=int(NumberOfPages)
            print('\nNUM reviews to scrape',NumberOfReviews,'\n')
                
        except:
            NumberOfPages=50
            if not NumberOfReviews>0:
                NumberOfReviews=''
           
        
        try:
            for i in range(1,NumberOfPages):#make while loop or break statement,
                #url redirects to main once all review pages
                newurl=masterurl[:insertplace]+'-or'+str(10*i)+masterurl[insertplace:]#all following results
                PageData=Read_A_Page_Reviews(masterurl,newurl,RestaurantName,RestaurantLocation)
                try:
                    RestaurantData=pd.concat([RestaurantData, PageData], axis=0,ignore_index=True)#ignore_index makes it easier to manipulste in Excel
                except Exception as e:
                    print('Error in concatting dataframe for',newurl,e,' on page',i)                
        except:
            print('Max pages at page',i+1)
    except:
        print('exception')
        RestaurantName=''
        RestaurantLocation=''
   
   
    return RestaurantData,NumberOfReviews,RatingList,RestaurantName,RestaurantLocation
def Read_A_Page_Reviews(masterurl,url,RestaurantName,RestaurantLocation): 
    driver.get(url)
    try:
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="onetrust-accept-btn-handler"]' ))).click()#click accept cookies banner
    except:
        a=1#print('Error accepting cookies')    
    try:
        for i in range(0,19):#could have a response for each review so need double           
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,"//*[text()='More']"))).click()
    except:
#selenium depreciated caused errors couldn't select by, found how to do another way by text()
        a=1               
    ScrapedData= driver.find_elements_by_class_name('review-container')
    num=0
    PageReviewDates=[]
    PageReviewText=[]
    PageReviewTitle=[]
    PageReviewMobile=[]
    PageVisitDates=[]
    PageResponses=[]
    PageResponseDates=[]
    for i in ScrapedData:    
        num=num+1
        i=i.text
        holder=i
        ReviewDate=i[i.find('Reviewed')+9:]
        ReviewDate=ReviewDate[:ReviewDate.find('\n')]
        PageReviewDates.append(ReviewDate)
        i=i[i.find('Reviewed '):]
        i=i[i.find('\n')+1:]
        i=i[:i.find('Helpful?')-1]
        temp=i.find('via mobile')
        if temp!=-1:#mobile statement correction from string to boolean field as not always there have to correct rest of text
            i=i[temp+1:]
            i=i[i.find('\n')+1:]
            PageReviewMobile.append(True)
        else:
            PageReviewMobile.append(False)
        temp=i.find('Date of visit: ')
        if temp!=-1:
            VisitDate=i[temp+15:]
            temp=VisitDate.find('\n')
            if temp!=-1:
                VisitDate=VisitDate[:temp]
        else:
            VisitDate=''
        PageVisitDates.append(VisitDate)
        ShowLessExist=i.find('Show less')

        if ShowLessExist!=-1:
             i=i[:ShowLessExist-1]
        Title=i[:i.find('\n')]
        PageReviewTitle.append(Title)
        i=i[i.find('\n')+1:]
        
        tempPageReviewText=i
        #i has been cut down to be the text, reset to original
    ###now scrape responses
        i=holder##key here resetting i
        i=i[i.find('Helpful?')+2:]
        i=i[i.find('\n')+1:]
        i=i[i.find('Responded '):]
        
        i=i[10:]
    #ResponseDate
        ResponseDate=i[:i.find('\n')]#i is now the response, just have to ensure all review text is not an earlier response
        i=i[i.find('\n')+1:]
        i=i[:i.find('Show less')-1]######key 1 here
        
        tempPageReviewText= tempPageReviewText.replace(i,'')
        PageReviewText.append(tempPageReviewText)
        num=num+1
        if len(i)>15:
            PageResponses.append(i)
        else:
            PageResponses.append('')
        if len(ResponseDate)>5 and len(ResponseDate)<20:
            PageResponseDates.append(ResponseDate)
        else:
            PageResponseDates.append('')   
        temp=i.find('Date of visit:')
        if temp!=-1:
            i=i[:temp-1]
        temp=i.find('Date of visit:')#duplicated on purpose as sometimes 1 didn't work
        if temp!=-1:
            i=i[:temp-1]
        
 
    PageData={'Restaurant_URL':masterurl,
                  'Restaurant_Name':RestaurantName,
                  'Restaurant_Location':RestaurantLocation,
                              
                  'Restaurant_Review_Text':PageReviewText,
                  'Restaurant_Review_Title':PageReviewTitle,
                  'Restaurant_Review_Date':PageReviewDates,
                  'Restaurant_Review_Mobile_Review?':PageReviewMobile,
                  'Restaurant_Visit_Date':PageVisitDates,
                  'Restaurant_Responses':PageResponses,
                  'Restaurant_Response_Date':PageResponseDates#define dataframe at lowest level to prevent length of array errors breaking entire dataframe causing data loss
                                             
                              
                        }
    PageData=pd.DataFrame(PageData)
    return PageData
        
InitialURLscrape()  
driver.quit()