# -*- coding: utf-8 -*-
"""
Program 2/2 to analyse web scraper data
Created on Tue Jun  7 20:37:58 2022

@author: layto
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime
from scipy import stats
plt.style.use('dark_background') 
def NLP():
    WebScraperNumber='168'
    ReviewData=pd.read_csv('ALLRestaurantsData'+WebScraperNumber+'.csv')
    ReviewData['Restaurant_Review_Text']=ReviewData['Restaurant_Review_Title']+' '+ReviewData['Restaurant_Review_Text']
    ReviewData['Health_Inspector_Data']=ReviewData['Restaurant_Review_Text'].str.count('Food Poisoning| bugs| poisoning| diarrhoea| blood| vomiting| raw| unsanitary')
    ReviewData['Very_Negative_Words']=ReviewData['Restaurant_Review_Text'].str.contains('Food Poisoning| bugs| poisoning| diarrhoea| blood| vomiting| raw| unsanitary| cheated| salmonella| WORST| never arrived| insult| awful| terrible| disgrace| rude| ruined| worst| very expensive| short staff| disappointing| painful| embarrassing| pricey') 
    ReviewData['Negative_Words']=ReviewData['Restaurant_Review_Text'].str.contains(' cold| bland| underwhelming| awkward| mistake| burnt| burned| undercooked| unprofessional| untrained| rushed| bad| uncomfortable| mess| uncomfortable| expensive| late| slow') 
    
    ReviewData['Negative_Words']= (ReviewData['Negative_Words']) &  (ReviewData['Restaurant_Review_Text'].str.contains(' raw fish| cold beer') ==False)#lots of false positives for raw   so remove raw fish
    ReviewData['Positive_Words']=ReviewData['Restaurant_Review_Text'].str.contains(' cold beer| brilliant| great| quick| good| nice| cool| enjoyable| tasty| pleasant| fresh| helpful| decent| polite| right| friendly| kind| smiley| welcoming| value for money| accomodated|wide choice| reasonable') 
    ReviewData['Very_Positive_Words']=ReviewData['Restaurant_Review_Text'].str.contains(' iconic| best| wonderful| fantastic| phenomenonal| loveliest| attentive| amazing| amasing| gorgeous| excellent') 
    ReviewData['RankingSystem']=0
    ReviewData['RankingSystem'] = ReviewData.apply(lambda x: -2+x.RankingSystem if x.Very_Negative_Words == True else 0, axis=1)
    ReviewData['RankingSystem'] = ReviewData.apply(lambda x: -1+x.RankingSystem if x.Negative_Words == True else x.RankingSystem, axis=1)
    ReviewData['RankingSystem'] = ReviewData.apply(lambda x: 1+x.RankingSystem if x.Positive_Words == True else x.RankingSystem, axis=1)
    ReviewData['RankingSystem'] = ReviewData.apply(lambda x: 2+x.RankingSystem if x.Very_Positive_Words == True else x.RankingSystem, axis=1)

    ReviewData['RankingSystem'] =(ReviewData['RankingSystem']--3)/(3--3)#min max eq logic 
    ReviewData['RankingSystem'] =(ReviewData['RankingSystem']*4)+1#now min is 1 and max is 5 like restaurant whole as a range, there is decimals but this does not matter as we are aggregating to the restaurant level to compare
    del ReviewData['Unnamed: 0']
    ReviewData = ReviewData.reset_index(drop=True)  
    ReviewData=ReviewData.drop_duplicates()#duplicates due to url redirects on tripadvisor, this has since been patched on the webscraper
    ReviewData.to_csv('ReviewDataDetail'+str(len(ReviewData))+'.csv', index=False,encoding='utf-8-sig')
        
    ReviewDataAgg= ReviewData.groupby(['Restaurant_URL'], as_index=False).RankingSystem.mean()#as_index crucial here

    ReviewDataAgg["Rank"] = ReviewDataAgg["RankingSystem"].rank(ascending=False)
    ReviewDataAgg["Percentile"] = ReviewDataAgg["RankingSystem"].rank(ascending=False,pct=True)*100
    ReviewDataAgg["Percentile"] = ReviewDataAgg["Percentile"].round(decimals=1)
    
    #now want to compare score to original and show distribution, e.g. leftshift right shift and explain if NLP improved metric or at least reasonable to use on an aggregated level
    RestaurantData=pd.read_csv('ALLRestaurantsURLs'+WebScraperNumber+'.csv')
    Columns=['RatedExcellent','RatedVery_good','RatedAverage','RatedPoor','RatedTerrible']
    RestaurantData['Number_of_Reviews_by_Restaurant']=0
    for i in Columns:
        try:
            RestaurantData[i]=RestaurantData[i].str.replace(',','')#issue with commas on website and then a datatype issue  
            RestaurantData[i]=RestaurantData[i].astype(float)
        except:
            a=1
        RestaurantData['Number_of_Reviews_by_Restaurant']=RestaurantData[i]+RestaurantData['Number_of_Reviews_by_Restaurant']
    RestaurantData['AVG_original_rating']=(RestaurantData['RatedExcellent']*5+RestaurantData['RatedVery_good']*4+RestaurantData['RatedAverage']*3+RestaurantData['RatedPoor']*2+RestaurantData['RatedTerrible'])/RestaurantData['Number_of_Reviews_by_Restaurant']                                                     
    RestaurantData['AVG_original_rating']= RestaurantData['AVG_original_rating'].round(decimals=3)
    RestaurantData['original_rank']=RestaurantData['AVG_original_rating'].rank(ascending=False)
    RestaurantData['original_percentile']=RestaurantData['AVG_original_rating'].rank(ascending=False,pct=True)*100
    RestaurantData['original_percentile']=RestaurantData['original_percentile'].round(decimals=1)
    
    RestaurantData=RestaurantData[ RestaurantData['RatedVery_good'].isnull()==False]#filter out corrupted restaurants, corrected in later webscraper versions
    RestaurantData = RestaurantData.reset_index(drop=True)
    RestaurantData=RestaurantData.drop_duplicates()#precautionary for future datasets or if people combine scraped datasets
    Errordata=pd.merge(ReviewDataAgg, RestaurantData, on=None, left_on="Restaurant_URL", right_on="URL",  how="right", indicator=True)#right outer join pandas step 1
    Errordata=Errordata[Errordata['_merge']=='right_only']#right outer join step 2 using indicator column
    print('URLs with a restaurant level scraping error\n',Errordata.URL.unique())
    #occasionally the web scraper has corrupted and restaurants are missing but only ~1-5% of total scraped data is lost we have shown this via the errordata print statement
    ALLdata=pd.merge(ReviewDataAgg, RestaurantData, on=None, left_on="Restaurant_URL", right_on="URL",  how="inner")
    
    ALLdata = ALLdata.reset_index(drop=True)
    
    ALLdata=ALLdata[ALLdata['RatedVery_good'].notnull()]
    ALLdata['Rank Change new-original']=ALLdata['Rank']-ALLdata['original_rank']
    ALLdata['Percentile Change new-original']=ALLdata['Percentile']-ALLdata['original_percentile']
    
    plt.figure(figsize=(10,8),dpi=50)
    GRAPHdata=ALLdata[['original_rank','Rank']]#as expected positive relationship but i am interested in variance and quality of positive relationship, should be generally positive, wide outliers could be low sample size, fake reviews or unusually low length of reviews
    graph= sns.pairplot(GRAPHdata, kind='reg',diag_kind='kde' , plot_kws={'line_kws':{'linewidth':7,'color':'r'}, 'scatter_kws': {'alpha': 0.6,'s':80,'edgecolor':'k'}})
    GRAPHdata=ALLdata[['AVG_original_rating','RankingSystem','Rank Change new-original','Number_of_Reviews_by_Restaurant']]
    graph= sns.pairplot(GRAPHdata, kind='reg',diag_kind='kde' , plot_kws={'line_kws':{'linewidth':7,'color':'r'}, 'scatter_kws': {'alpha': 0.6,'s':80,'edgecolor':'k'}})
    GRAPHdata=ALLdata[['AVG_original_rating','RankingSystem']]
    graph= sns.pairplot(GRAPHdata, kind='reg',diag_kind='kde' , plot_kws={'line_kws':{'linewidth':7,'color':'r'}, 'scatter_kws': {'alpha': 0.6,'s':80,'edgecolor':'k'}})
    slope, intercept, r_value, p_value, std_err = stats.linregress(GRAPHdata['AVG_original_rating'],GRAPHdata['RankingSystem'])
    print('Linear Regression equation of original rating to new rating',str(slope)+'x+',intercept)
    
    
    ALLdata.to_csv('AllRankedOriginalRankingAndNLPrankingData'+str(len(ReviewData))+'.csv', index=False,encoding='utf-8-sig')
    #rank and percentile comparison between original and new rating
 
   #my highly rated and lowest rated values seemed to have the largest difference which implies my model preforms poorly around the extremes of the data range
    
   #secondary analysis, filter dates to last 3 months and cases of food poisoning or an increase of ratings from overall NLP to the last 3 months (NLP)
    ReviewData['Date'] = pd.to_datetime(ReviewData['Restaurant_Visit_Date'],format='%b-%y', errors='coerce')#b very important as adjust mar into March
    ReviewData['Last_6_Months']=ReviewData['Date'] >= (pd.Timestamp.now()-pd.DateOffset(months=6))
     
    #now helath analysis and change over last few months
    ReviewData['Number_reviews_Unsanitary']= ReviewData.apply(lambda x: 1 if x.Health_Inspector_Data == True else 0, axis=1)
    ReviewData['Count']=1
    ReviewData['last_6_months_Number_reviews_Unsanitary']= ReviewData.apply(lambda x: 1 if x.Number_reviews_Unsanitary== True and x.Last_6_Months== True else 0, axis=1)
    ReviewData['last_6_months_Number_Of_reviews']= ReviewData.apply(lambda x: 1 if x.Last_6_Months== True else 0, axis=1)
    ReviewData['last_6_months_AVG_NLP_Ranking_System']= ReviewData.apply(lambda x: x.RankingSystem if x.Last_6_Months== True else 0, axis=1)
    ReviewData.to_csv('ReviewDataDetail'+str(len(ReviewData))+'.csv', index=False,encoding='utf-8-sig')
    
    #visualise analysis
    ReviewData['AVG_RankingSystem']=ReviewData['RankingSystem']
    HealthDataAgg = ReviewData.groupby(['Restaurant_URL'])['Number_reviews_Unsanitary','last_6_months_Number_reviews_Unsanitary','last_6_months_Number_Of_reviews','last_6_months_AVG_NLP_Ranking_System','AVG_RankingSystem'].sum().reset_index()
    HealthDataAgg['last_6_months_AVG_NLP_Ranking_System']=(HealthDataAgg['last_6_months_AVG_NLP_Ranking_System']/HealthDataAgg['last_6_months_Number_Of_reviews']).round(decimals=2)
    HealthDataAgg['Total_Number_Of_Reviews'] = HealthDataAgg['Restaurant_URL'].map(ReviewData['Restaurant_URL'].value_counts())
   
    HealthDataAgg['AVG_RankingSystem']=(HealthDataAgg['AVG_RankingSystem']/HealthDataAgg['Total_Number_Of_Reviews']).round(decimals=2)
    HealthDataAgg['AVG_RankingSystem_improvement']=HealthDataAgg['last_6_months_AVG_NLP_Ranking_System']-HealthDataAgg['AVG_RankingSystem']
    HealthDataAgg['ImprovingNLPrating']=HealthDataAgg['AVG_RankingSystem']<HealthDataAgg['last_6_months_AVG_NLP_Ranking_System']
 
    HealthDataAgg['Last_6_Months_Percentage_Unsanitary']=(HealthDataAgg['last_6_months_Number_reviews_Unsanitary']*100/HealthDataAgg['last_6_months_Number_Of_reviews']).round(decimals=2)
    HealthDataAgg['Last_6_Months_Percentile_Unsanitary']=HealthDataAgg['Last_6_Months_Percentage_Unsanitary'].rank(ascending=False,pct=True).round(decimals=3)*100
    
    HealthDataAgg['All_Percentage_Unsanitary']=((HealthDataAgg['Number_reviews_Unsanitary']/HealthDataAgg['Total_Number_Of_Reviews'])*100).round(decimals=2)
    HealthDataAgg['All_Percentile_Unsanitary']=HealthDataAgg['All_Percentage_Unsanitary'].rank(ascending=False,pct=True).round(decimals=4)*100
    HealthDataAgg['Cause_For_Concern_Dropping_Health_Standards']=HealthDataAgg['All_Percentage_Unsanitary']<HealthDataAgg['Last_6_Months_Percentage_Unsanitary']
    
    GRAPHdata=HealthDataAgg[HealthDataAgg['Number_reviews_Unsanitary']!=0]
    GRAPHdata=GRAPHdata[['AVG_RankingSystem','All_Percentage_Unsanitary']]
    sns.pairplot(GRAPHdata, kind='reg',diag_kind='kde' , plot_kws={'line_kws':{'linewidth':7,'color':'r'}, 'scatter_kws': {'alpha': 0.6,'s':80,'edgecolor':'k'}})
    GRAPHdata=HealthDataAgg[['AVG_RankingSystem','last_6_months_AVG_NLP_Ranking_System']]
    
    sns.pairplot(GRAPHdata, kind='reg',diag_kind='kde' , plot_kws={'line_kws':{'linewidth':7,'color':'r'}, 'scatter_kws': {'alpha': 0.6,'s':80,'edgecolor':'k'}})
    HealthDataAgg.to_csv('HealthData+RatingImprovement'+str(len(ReviewData))+'.csv', index=False,encoding='utf-8-sig')
    
  
NLP()