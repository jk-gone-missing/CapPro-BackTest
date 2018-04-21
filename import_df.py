import pandas as pd
import asset_class as ac
from datetime import datetime
import numpy
import time

def PriceExists(df, PriceDate):
    if PriceDate in df.index:
        return True
    else:
        return False

def DivPayableExists(df, PayDate):
    if PayDate in df.loc[:,'Payable Date']:
        return True
    else:
        return False


def UpdatePriceAndValue(price_date):
    spy.price=SPY_Price_DF.loc[price_date,'Close']
    agg.price=AGG_Price_DF.loc[price_date,'Close']
    spy.value=spy.holding*spy.price
    agg.value=agg.holding*agg.price

def WriteToDF(df,w_date):
    UpdatePriceAndValue #ensure values are updated.

def ProcessDividend(RunID,an_investment,dist_df,process_date):
    if PriceExists(dist_df,process_date):
        #Get the dividen rate
        div_rate= dist_df.loc[process_date,'Value']
        div_amount = round(div_rate*an_investment.holding,2)
        div_payable = dist_df.loc[process_date,'Payable Date']
        #Write a dividend accrual
        #Writes transaction to 
        query_data= (RunID,process_date.date(),an_investment.InvestmentID,'DividendAccrual',float(div_rate),float(an_investment.holding),float(div_amount),0.0)
        ac.query_transaction(query_data)
        an_investment.div_accrued+=div_amount
        # print ('RunID %s Dividend Accrual Added for %s at %s for amount %.2f payable %s' % (RunID,an_investment.name,process_date,div_amount,div_payable))


        #Write a future cash transaction
        query_data= (RunID,div_payable.date(),an_investment.InvestmentID,'DividendPaid',float(div_rate),float(an_investment.holding),float(div_amount),0.0)
        ac.query_transaction(query_data)
        #then cancel the accrual as at the same date - NB. value has a negative sign
        query_data= (RunID,div_payable.date(),an_investment.InvestmentID,'DividendAccCancel',float(div_rate),float(an_investment.holding),float(-div_amount),0.0)
        ac.query_transaction(query_data)

    #now check if curret date corresponds to payment date. PriceExists only looks at the index so 
    div_paid = ac.SelectQuery('SELECT tValue FROM transactions where RunID ="%s" AND tDate="%s" AND InvestmentID="%s" AND tType="DividendPaid"' % (RunID,process_date,an_investment.InvestmentID),True)
    if len(div_paid)>0:
        ac.asset.CashBalance+=div_paid[0][0]
        #remove accrual
        an_investment.div_accrued-=div_paid[0][0]


def WithinTolerance(e_pc,e_ab,tol_type="abs"):
    #tests whether the excess lists contain 
    #any values that are 'neg'ative that are in breach of the tolerance criteria
    #any values that are 'pos'ative that are in breach of the tolerance criteria
    #any values that are in 'abs'olute terms are in breach of the tolerance criteria
    if tol_type=='neg':
        if any([x<(-tolerance_pc) for x in e_pc]) and any([x<(-tolerance_ab) for x in e_ab]):
            return False
        else:
            return True
    elif tol_type=='pos':
        if any([x>tolerance_pc for x in e_pc]) and any([x>tolerance_ab for x in e_ab]):
            return False
        else:
            return True
    else:
        if any([abs(x)>tolerance_pc for x in e_pc]) and any([abs(x)>tolerance_ab for x in e_ab]):
            return False
        else:
            return True

def RebalanceForAPeriod(proj_start_date,LengthOfPeriod,RunID):
    rbfp_start=time.time()
    YearsToAdd=pd.to_timedelta(LengthOfPeriod, unit="Y")
    end_date=proj_start_date+YearsToAdd
    rng = pd.date_range(start=proj_start_date, end=end_date, freq='D')
    rng.name = "Proj_Date"

    OneDay=pd.to_timedelta(1, unit="D")

    while PriceExists(SPY_Price_DF,proj_start_date)==False:
        proj_start_date+=OneDay

    if PriceExists(AGG_Price_DF,proj_start_date):
        agg.price=AGG_Price_DF.loc[proj_start_date,'Close']
    else:
        print('DOH!')
    spy.price=SPY_Price_DF.loc[proj_start_date,'Close'] 


    spy_purchase=ac.asset.CashBalance*spy.target_allocation
    agg_purchase=ac.asset.CashBalance-spy_purchase
    spy.buy(RunID,proj_start_date,spy_purchase)
    agg.buy(RunID,proj_start_date,agg_purchase)
    ac.WriteBalances(RunID,proj_start_date)
    #next we need to try and iterate over the range... skipping days when there are no prices

    #iterate from the proj_start_date to the end of the range
    
    for p_date in rng:
        if p_date>proj_start_date:
            #add interest daily.
            ac.asset.CashBalance+=ac.asset.CashBalance*cash_interest/365
            #charges
            #deliberately done on last value available.
            ac.asset.CashBalance-=(spy.value+agg.value+ac.asset.CashBalance+spy.div_accrued+agg.div_accrued)*portfolio_charge/365


            #step 1 is to check for Ex Div dates. there must be holdings PRIOR to ex div in order to 
            #receive the distribution
            # if p_date==pd.datetime.strptime('2004-09-29', '%Y-%m-%d'):
            #     print ('here')
            ProcessDividend(RunID,agg,AGG_Dist_DF,p_date)
            ProcessDividend(RunID,spy,SPY_Dist_DF,p_date)

            #step 2 is to value. need to check if there are prices.
            if PriceExists(AGG_Price_DF,p_date) and PriceExists(SPY_Price_DF,p_date):
                #re-value 
                UpdatePriceAndValue(p_date)
                portfolio_value = spy.value+agg.value+ac.asset.CashBalance
                #test the values for 2% tolerance...
                #Calculate the excess in each asset.
                all_assets=ac.asset.instances
                all_values=[x.value for x in all_assets]
                all_targets=[x.target_allocation for x in all_assets]
                all_actual_allocations=[x/portfolio_value for x in all_values]
                excess_pc=[all_actual_allocations-all_targets for all_actual_allocations,all_targets in zip(all_actual_allocations,all_targets)]
                excess_val=[x*portfolio_value for x in excess_pc]
                total_negatives = sum([x*(x<0.0) for x in excess_val])
                starting_cash_balance = ac.asset.CashBalance
                #test to see if there are assets with less than tolerance in target
                if WithinTolerance(excess_pc,excess_val,'abs'):
                    #see if we can apply the cash over all assets
                    #Check whether cash balance is tradeable
                    if starting_cash_balance>tolerance_ab:
                        for an_asset in ac.asset.instances:
                            this_asset_ab = an_asset.value-portfolio_value*an_asset.target_allocation
                            buy_value=0.0
                            if this_asset_ab<0.0:
                                #the first part of the buy value is the deficiency in the allocation
                                buy_value += this_asset_ab*starting_cash_balance/total_negatives
                            if starting_cash_balance>abs(total_negatives):
                                # the second part of the allocaiton is the target proportion of the asset if there is any 
                                # cash left over from dealing with the deficiencies across the portfolio
                                buy_value+=(starting_cash_balance+total_negatives)*an_asset.target_allocation
                            if buy_value>tolerance_ab:
                                an_asset.buy(RunID,p_date,buy_value)
                else:
                    #Not within tolerance
                    #Check whether cash balance is tradeable
                    #now see if the cashbalance alone will fix the imbalance
                    #sum over the assets that are below tolerance

                    cash_to_apply= [starting_cash_balance/total_negatives*x*(x<0.0) for x in excess_val]
                    #now apply the cash to the excess
                    test_excess_val=[excess_val+cash_to_apply for excess_val,cash_to_apply in zip(excess_val,cash_to_apply)]
                    #create equivalent _pc list
                    test_excess_pc=[x/portfolio_value for x in test_excess_val]
                    if WithinTolerance(test_excess_pc,test_excess_val,'abs'):
                        #then only trading the cash is sufficient
                        #buy trades are as per cash_to_apply
                        for an_asset in ac.asset.instances:
                            this_asset_pc = an_asset.value/portfolio_value-an_asset.target_allocation
                            this_asset_ab = an_asset.value-portfolio_value*an_asset.target_allocation
                            if this_asset_pc<-tolerance_pc and this_asset_ab<-tolerance_ab:
                                buy_value= this_asset_ab/total_negatives*starting_cash_balance
                                an_asset.buy(RunID,p_date,buy_value)
                    else:
                        #cash alone will not fix the problem
                        #will need to sell some stock and trade the cash
                        for an_asset in ac.asset.instances:
                            this_asset_ab = an_asset.value-portfolio_value*an_asset.target_allocation
                            this_asset_pc = an_asset.value/portfolio_value-an_asset.target_allocation
                            if this_asset_ab>tolerance_ab and this_asset_pc>tolerance_pc:
                                #Sell asset to the estimate value of the excess.
                                an_asset.sell_est_value(RunID,p_date,this_asset_ab)
                        #Now we've generated enough cash to be able to fix the problem... hopefully and we can reuse the 
                        #code above from when we had enough cash at the start to fix it.
                        #total negatives is still the same bur starting cash balance needs updating for the cash generated from 
                        #the sells.
                        starting_cash_balance=ac.asset.CashBalance
                        for an_asset in ac.asset.instances:
                            this_asset_pc = an_asset.value/portfolio_value-an_asset.target_allocation
                            this_asset_ab = an_asset.value-portfolio_value*an_asset.target_allocation
                            if this_asset_pc<-tolerance_pc and this_asset_ab<-tolerance_ab:
                                buy_value= this_asset_ab/total_negatives*starting_cash_balance
                                an_asset.buy(RunID,p_date,buy_value)
                                
                #Write Balances to database
                ac.WriteBalances(RunID,p_date)

                #want to find the end of year values ... the last value on or before proj_start_date + n*Years

    rbfp_end=time.time()
    print("%s Elapsed time was %g seconds" % (RunID,rbfp_end - rbfp_start)) 
    return proj_start_date



#EXECUTABLE CODE STARTS HERE
vixfile='/Users/jpkemp/Documents/CapPro/Data/VIX.csv'
aggprices='/Users/jpkemp/Documents/CapPro/Data/AGG.csv'
aggdist='/Users/jpkemp/Documents/CapPro/Data/AGG Distributions 09042018.csv'
spyprices = '/Users/jpkemp/Documents/CapPro/Data/SPY.csv'
spydist = '/Users/jpkemp/Documents/CapPro/Data/spy dividends.csv'

dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
dateparse2 = lambda x: pd.datetime.strptime(x, '%d/%m/%Y')
VIX_DF=pd.read_csv(vixfile, parse_dates=True)
AGG_Price_DF=pd.read_csv(aggprices, parse_dates=['Date'], date_parser=dateparse)
SPY_Price_DF=pd.read_csv(spyprices, parse_dates=['Date'],date_parser=dateparse2)
VIX_DF = VIX_DF.set_index(['Date'])
AGG_Price_DF = AGG_Price_DF.set_index(['Date'])
SPY_Price_DF = SPY_Price_DF.set_index(['Date'])
AGG_Price_DF.sort_index(axis=0, ascending=True, inplace=True)
AGG_Dist_DF=pd.read_csv(aggdist,parse_dates=['Ex-Date','Payable Date'],date_parser=dateparse2)
AGG_Dist_DF = AGG_Dist_DF.set_index(['Ex-Date'])
SPY_Dist_DF=pd.read_csv(spydist,parse_dates=['Ex-Date','Payable Date'],date_parser=dateparse2)
SPY_Dist_DF = SPY_Dist_DF.set_index(['Ex-Date'])
#AGG_Price_DF.rename(columns={'close':'Close'}, inplace=True)

#Get the earliest of the two last dates
last_common_price_date=min(AGG_Price_DF.iloc[-1].name,SPY_Price_DF.iloc[-1].name)

#Clean the test database
ac.CleanTestData()

# #Adjust run parameters here !!!!!!!!!!!!!!!!!!!!    work for 2 and 5

#if use_max_rangeis set to True then years_of_start_months is ignored
use_max_range=True
years_of_start_months=2


number_of_projection_years=5
tolerance_pc=0.02
tolerance_ab=50.
initial_cash_value=10000
equity_pc=0.5
bond_pc=0.5
cash_interest=0.01
portfolio_charge=0.0

ProjYears_td=pd.to_timedelta(number_of_projection_years, unit="Y")
StartYear_td=pd.to_timedelta(years_of_start_months, unit="Y")
OneDay=pd.to_timedelta(1,unit='D')
last_possible_start=last_common_price_date-ProjYears_td-OneDay

first_start_date='2003-12-31'
first_start_date=pd.datetime.strptime(first_start_date, '%Y-%m-%d')

if use_max_range:
    # 
    end_date=last_possible_start
else:
    end_date=first_start_date+StartYear_td

start_rng = pd.date_range(start=first_start_date, end=end_date, freq='3M')
start_rng.name = "RangeStart_Date"

#Initialise position -  this needs a start date

spy=ac.asset("SPY ETF",'SPY001',0,equity_pc,0,0,0.0)
agg=ac.asset("BarclaysAgg ETF",'AGG001',0,bond_pc,0,0,0.0)
#get prices spy and agg at those dates

for start_date in start_rng:
    
    ProjRunID='test_'+start_date.strftime('%Y%m%d')
    
    ac.asset.CashBalance=initial_cash_value
    for an_aset in ac.asset.instances:
        an_aset.holding=0.0
        an_aset.value=0.0
        an_aset.div_accrued=0.0

    actual_start=RebalanceForAPeriod(start_date,number_of_projection_years,ProjRunID)
    

# #Calculate Standard Deviations


