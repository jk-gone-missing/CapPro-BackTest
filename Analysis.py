import pandas as pd
from datetime import datetime
import numpy as np
import time
import pymysql
import matplotlib
from matplotlib import pyplot as plt
from IPython.display import set_matplotlib_formats
from scipy.stats import norm

kw_save = dict(bbox_iches='tight', transparent=True)

def databaseconnector():
    return  pymysql.connect(user='root', password='actuary',
                                host='localhost',
                                database='cppi_test')



def SelectQuery(my_text,return_value):
    cnx=databaseconnector()
    cursor=cnx.cursor()
    cursor.execute(my_text)

    if return_value:
        return cursor.fetchall()
    else:
        cnx.commit()
    cursor.close
    cnx.close




def GetRunIDsForTimePeriod(datestring):
    qs =  'select DISTINCT RunID from TotalBalance where RunID LIKE %s ' % ('"%'+datestring+'"')
    return SelectQuery(qs,True)

def GetBalsForRunID(RunID,s_date,e_date):
    qs =  'select bDate,Total as Close from TotalBalance where RunID= %s AND bDate>=%s AND bDate<%s' % ('"'+RunID+'"','"'+s_date+'"','"'+e_date+'"')
    # print (qs)
    return SelectQuery(qs,True)

def TableManagement():
    ExistingTables = SelectQuery('SHOW TABLES',True)
    df= pd.DataFrame(list(ExistingTables))
    df.columns=['tables']

    if 'SE_Dates'  in list(df.tables):
        
        SelectQuery('DROP TABLE SE_Dates',False)
    SelectQuery('create table SE_Dates select RunID,min(bDate) as StartDate, max(bDate) as EndDate from balances group by RunID',False)

    if 'InvBalance'  in list(df.tables):
        SelectQuery('DROP TABLE InvBalance',False)
    SelectQuery('CREATE TABLE InvBalance Select RunID, bDate, InvestmentID,Sum(bValue) as Total,SUM(Proportion) as Proportion, SUM(tProp) as tProp from balances GROUP BY RunID,bDate,InvestmentID',False)


    if 'TotalBalance'  in list(df.tables):
        SelectQuery('DROP TABLE TotalBalance',False)
    SelectQuery('CREATE TABLE TotalBalance Select RunID, bDate, Sum(Total) as Total from InvBalance GROUP BY RunID,bDate',False)

def PlotForIDs(id_list, output_file,s_date,e_date):
    y=0
    for x in id_list:
        b_df=pd.DataFrame(list(GetBalsForRunID(x,s_date,e_date)))
        b_df.columns=['Date','Close']
        b_df = b_df.set_index(['Date'])
        plt.plot(b_df,linewidth=1.0, label=x)
        y+=1
    b_df['Close'] = b_df['Close'].apply(lambda x: 45000.0)
    plt.plot(b_df,linewidth=1.0,label='Floor')
    
    plt.legend(loc='upper left')
    print (y)
    plt.savefig(output_file, **kw_save)

def GetMinAndMaxDatesForID(RunID):
    return SelectQuery('Select  min(bDate) as StartDate, max(bDate) as EndDate from TotalBalance WHERE RunID=%s' % '"'+RunID+'"',True)


#Execution
def CreateTablesAndCharts(datestring):
    # TableManagement()
    r = list(GetRunIDsForTimePeriod(datestring))

    r1 = [x[0] for x in r]
    print (r1)
    output_file='/Users/jpkemp/Documents/CapPro/focus1.png'
    output_file2='/Users/jpkemp/Documents/CapPro/full.png'


    plt.rcParams["figure.figsize"] = [18.0,9.0]

    minmaxdates=pd.DataFrame(list(GetMinAndMaxDatesForID(r1[0])))
    minmaxdates.columns=['StartDate','EndDate']

    # PlotForIDs(r1,output_file, '2008-06-30', '2008-12-31')

    PlotForIDs(r1,output_file2, minmaxdates.loc[0,'StartDate'].strftime('%Y/%m/%d'), minmaxdates.loc[0,'EndDate'].strftime('%Y/%m/%d'))


def CreateStackChart(RunID):
    #Creates a stacked bar chart for an individual scenario

    #Query InvBalance for RunID and returnt the results
    Assets=pd.DataFrame(list(SelectQuery('Select bDate,InvestmentID,Total from InvBalance WHERE RunID=%s' %'"'+RunID+'"',True)))
    #Select only specific dates from the data - better to pass to the query....
    #eg from the first date get quarterly sample points - better to passs this in as a list then we have more flexibulity
    
    
    # do the plot!
    print (Assets.head())

def DateRangeForStack(RunID):
    #Get start date and end Date from SE_Dates
    x=list(SelectQuery('Select StartDate,EndDate from SE_Dates WHERE RunID=%s' %'"'+RunID+'"',True))[0]
    
    sample_rng = pd.date_range(start=x[0], end=x[1], freq='3M')
    date_sample=[]
    for  y in sample_rng:
        date_sample.append(y.date())
    if not(date_sample[-1]==x[1]):
        date_sample.append(x[1])
    
    #
    SelectQuery('DELETE FROM tempRange',False)
    for mydate in date_sample:
        query=('INSERT INTO tempRange (daterange) VALUES (%s)' % ('"'+str(mydate)+'"'))
        SelectQuery(query,False)
    
    #query for the values at dates now in tempRange
    query='SELECT t2.bDate, t2.InvestmentID,t2.Total FROM tempRange t1 INNER JOIN InvBalance t2 ON (t2.bDate = t1.daterange) WHERE t2.RunID=%s' % '"'+RunID+'"'
    Stack_df=pd.DataFrame(list(SelectQuery(query,True)))
    Stack_df.columns=['Date','InvestmentID','Value']
    
    new_df=pd.pivot_table(Stack_df, values='Value', index=['Date'], columns=['InvestmentID'], aggfunc=np.sum)


    plt.rcParams["figure.figsize"] = [18.0,9.0]
    ind=range(len(new_df.index))
    p1 = plt.bar(ind, new_df.AGG001, color='r')
    p2 = plt.bar(ind, new_df.SPY001, color='b',bottom=new_df.AGG001)
    p3 = plt.bar(ind, new_df.CashBal, color='g',bottom=new_df.SPY001+new_df.AGG001)
    plt.xticks(ind,date_sample)
    plt.legend((p1[0], p2[0],p3[0]), ('AGG', 'SPY','Cash'))
    plt.legend(loc='upper left')
    
    output_file='/Users/jpkemp/Documents/CapPro/stack1.png'
    plt.savefig(output_file, **kw_save)

def VIX_Analysis():
    vixfile='/Users/jpkemp/Documents/CapPro/Data/VIX.csv'
    dateparse = lambda x: pd.datetime.strptime(x, '%Y/%m/%d')
    VIX_DF=pd.read_csv(vixfile, parse_dates=['Date'], date_parser=dateparse,usecols=['Date','Close'])
    VIX_DF = VIX_DF.set_index(['Date'])

    output_file='/Users/jpkemp/Documents/CapPro/VIX1.png'
    plt.rcParams["figure.figsize"] = [18.0,9.0]
    plt.plot(VIX_DF,linewidth=1.0)
    plt.savefig(output_file, **kw_save)
    v_list=list(VIX_DF['Close'])
    v_list.sort()
    v_count=len(v_list)
    v_pv=v_count/100
    nintey_5=int(v_pv*95)
    print (v_list[nintey_5])
    
    




VIX_Analysis()
# TableManagement()
# CreateStackChart('Test50_20031231')
# DateRangeForStack('TestCPPI50_20040630')
# CreateTablesAndCharts('20070930')


#Read the balances from the rebalanced and correlate with VIX....
#and look at the percentiles of VIX

