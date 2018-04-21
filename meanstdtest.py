import numpy
import pandas as pd
import mysql.connector

import asset_class  as ac

def AnniversaryDate(first_date, n_Years):
    AnnDates=[first_date]

    for nYear in range(1,n_Years+1,1):
        AnnDates.append(first_date+pd.to_timedelta(nYear, unit="Y"))
    return AnnDates


def query_get_value_at_date(rID, v_date):
    #returns the portfolio value at a give date or the nearest date before
    query=('SELECT bValue FROM balances WHERE RunID="%s" AND bDate="%s"'%(rID,v_date))
    return SelectQuery(query,True)



def SelectQuery(my_text,return_value):
    cnx=  mysql.connector.connect(user='root', password='actuary',
                                host='localhost',
                                database='cppi_test')
    cursor=cnx.cursor()
    cursor.execute(my_text)

    if return_value:
        return cursor.fetchall()
    else:
        cnx.commit()
    cursor.close
    cnx.close
    

def ValueAtDateOrLatestAvailable(Rid,value_date):
    #start with a date conver to text but if null return then change the date and reconvert and query

    OneDay=pd.to_timedelta(1, unit="D")

    bFound=False
    while bFound==False:
        therows=(query_get_value_at_date(Rid,value_date))
        if len(therows)>0:
            bFound=True
        else:
            value_date-=OneDay

    #the return is a list of tuples.. list allows for each row being returned as a tuple
    #in this case I'm only returning a single value from each row.
    #we can detect a null return via the length.

    x=(sum(y) for y in therows)
    z=sum(x)
    return z

def InsertIntoSummary(data):
    cnx = mysql.connector.connect(user='root', password='actuary',
                                host='localhost',
                                database='cppi_test')
    cursor = cnx.cursor()
    query=("INSERT INTO summary "
               "(RunID, LengthOfProjection,AverageReturn, Volatility) "
               "VALUES (%s, %s, %s, %s)")
    cursor.execute(query,data)
    cnx.commit()
    cursor.close
    cnx.close

SelectQuery('DELETE FROM summary',False)
#get the earliest date for each runid - then get the start, anniversary and final values
query='select RunID, min(bDate), max(bDate) from balances group by RunID'
ids_and_dates=SelectQuery(query,True)

#ids_and_dates is a list of tuples

print(ids_and_dates)
r_count=0
for projection_set in ids_and_dates:
    r_count+=1
    first_run=projection_set #this returns a tuple with three elements , RunID, Min bDate, Max bDate
    first_id= first_run[0]
    first_start_date=first_run[1]
    first_end_date=first_run[2]

    period=first_end_date-first_start_date
    number_of_projection_years=int(round(period.days/365.25,0))
    
    #Now create the list of anniversaries
    anniversary_range= AnniversaryDate(first_start_date,number_of_projection_years)

    ann_values=[]
    for ann_date in anniversary_range:
        ann_values.append(ValueAtDateOrLatestAvailable(first_id,ann_date))


    ann_returns=[]
    
    for x in range(1,number_of_projection_years+1,1):
        ann_returns.append(ann_values[x]/ann_values[x-1]-1)

    ann_returns=numpy.array(ann_returns)
    std_returns=round(numpy.std(ann_returns),4)
    av_return = round(((ann_values[number_of_projection_years]/ann_values[0])**(1/number_of_projection_years)-1),4)
    print ("average return= %.4f standard deviation= %.4f" %(av_return,std_returns))
    InsertIntoSummary((first_id,float(number_of_projection_years),float(av_return),float(std_returns)))

