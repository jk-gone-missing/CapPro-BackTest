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
    for x in ac.asset.instances:
        x.price = x.pdf.loc[price_date,'Close']
        x.value = x.price * x.holding

def WriteToDF(df,w_date):
    UpdatePriceAndValue #ensure values are updated.

def ProcessDividend(RunID,process_date):
    for an_investment in ac.asset.instances:
        dist_df=an_investment.ddf
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


def CommonPriceExists(thisDate):
    flag=True
    for x in ac.asset.instances:
        if not(PriceExists(x.pdf,thisDate)):
            flag=False
    return flag

def InvestSpareCash(RunID,p_date,starting_cash_balance,portfolio_value,total_negatives):
    if starting_cash_balance>tolerance_ab:
        for an_asset in ac.asset.instances:
            this_asset_ab = an_asset.value-portfolio_value*an_asset.target_allocation
            buy_value=0.0
            if this_asset_ab<0.0:
                #the first part of the buy value is the deficiency in the allocation
                if starting_cash_balance>=abs(total_negatives):
                    buy_value+=(-this_asset_ab)
                else:
                    buy_value += this_asset_ab*starting_cash_balance/total_negatives
                an_asset.buy(RunID,p_date,buy_value)


def Rebalance(RunID,p_date,starting_cash_balance,portfolio_value,total_negatives,excess_val):
    
    #Do the sells first
    for an_asset in ac.asset.instances:
        this_asset_pc = an_asset.value/portfolio_value-an_asset.target_allocation
        this_asset_ab = an_asset.value-portfolio_value*an_asset.target_allocation
        if this_asset_pc>tolerance_pc and this_asset_ab>tolerance_ab:
            #The position is bigger than the target by the % tolerance AND the absolute tolerance.
            an_asset.sell_est_value(RunID,p_date,this_asset_ab)

    #Now do the buys
    for an_asset in ac.asset.instances:
        this_asset_pc = an_asset.value/portfolio_value-an_asset.target_allocation
        this_asset_ab = an_asset.value-portfolio_value*an_asset.target_allocation
        if this_asset_pc<-tolerance_pc and this_asset_ab<-tolerance_ab:
            #The position is smaller than the target by the % tolerance AND the absolute tolerance.
            an_asset.buy(RunID,p_date,min(-this_asset_ab,ac.asset.CashBalance))
    

def RebalanceForAPeriod(proj_start_date,LengthOfPeriod,RunID):
    rbfp_start=time.time()
    YearsToAdd=pd.to_timedelta(LengthOfPeriod, unit="Y")
    end_date=proj_start_date+YearsToAdd
    rng = pd.date_range(start=proj_start_date, end=end_date, freq='D')
    rng.name = "Proj_Date"

    OneDay=pd.to_timedelta(1, unit="D")

    #Get the first date that a price exists for all assets

    while CommonPriceExists(proj_start_date)==False:
        proj_start_date+=OneDay
    
    UpdatePriceAndValue(proj_start_date)

    if cppi_run:
        SetCPPITargets(ac.asset.CashBalance,proj_start_date) 
    starting_cash_balance = ac.asset.CashBalance
    #make initial purchases
    for x in ac.asset.instances:
        x.buy(RunID,proj_start_date,starting_cash_balance*x.target_allocation)

    ac.WriteBalances(RunID,proj_start_date)
    #next we need to try and iterate over the range... skipping days when there are no prices

    #iterate from the proj_start_date to the end of the range
    
    for p_date in rng:
        if p_date>proj_start_date:
            #add interest daily.
            # if ac.asset.CashBalance<0:
            #     print (ac.asset.CashBalance)
            ac.asset.CashBalance+=round(ac.asset.CashBalance*cash_interest/365,2)
            #charges
            #deliberately done on last value available.
            ac.asset.CashBalance-=round(ac.GetPortfolioValue()*portfolio_charge/365,2)
            #step 1 is to check for Ex Div dates. there must be holdings PRIOR to ex div in order to 
            #receive the distribution

            ProcessDividend(RunID,p_date)

            #step 2 is to value. need to check if there are prices.
            if CommonPriceExists(p_date):
                #re-value 
                UpdatePriceAndValue(p_date)
                portfolio_value = ac.GetPortfolioValue()

                #reset targets based on updated valuation
                if cppi_run:
                    SetCPPITargets(portfolio_value,p_date) 
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
                if rebal_flag:
                    if WithinTolerance(excess_pc,excess_val,'abs'):
                        #see if we can apply the cash over all assets
                        InvestSpareCash(RunID,p_date,starting_cash_balance,portfolio_value,total_negatives)
                    else:
                        #Not within tolerance
                        Rebalance(RunID,p_date,starting_cash_balance,portfolio_value,total_negatives,excess_val)
                else:
                    #Just apply spare cash
                    InvestSpareCash(RunID,p_date,starting_cash_balance,portfolio_value,total_negatives)
                                
                #Write Balances to database
                ac.WriteBalances(RunID,p_date)

                #want to find the end of year values ... the last value on or before proj_start_date + n*Years

    rbfp_end=time.time()
    print("%s Elapsed time was %g seconds" % (RunID,rbfp_end - rbfp_start)) 
    return proj_start_date

def SetUpAsset(AssName, AssID,PriceFile, DistributionFile):
    
    dateparse = lambda x: pd.datetime.strptime(x, '%d/%m/%Y')
    
    Price_DF=pd.read_csv(PriceFile, parse_dates=['Date'], date_parser=dateparse, usecols=['Date','Close'])
    Price_DF = Price_DF.set_index(['Date'])
    Price_DF.sort_index(axis=0, ascending=True, inplace=True)

    Dist_DF=pd.read_csv(DistributionFile,parse_dates=['Ex-Date','Payable Date'],date_parser=dateparse, usecols=['Ex-Date','Payable Date','Value'])
    Dist_DF = Dist_DF.set_index(['Ex-Date'])

    return ac.asset(AssName,AssID,0,0.0,0,0,0.0,Price_DF,Dist_DF)

def SetCPPITargets(p_value,p_date):
    
    if vm_flag:
        lf_adj=vix_modulation(p_date)
    else:
        lf_adj=1.0
    cushion = p_value-floor_abs
    risk_abs = cushion*lev_factor*lf_adj
    risk_pc = risk_abs/p_value
    for an_aset in ac.asset.instances:
        an_aset.target_allocation=round(risk_pc*0.50,4) #to to change and add another variable

def vix_modulation(p_date):
    
    
    
    OneDay=pd.to_timedelta(1,unit='D')
    Vix_date=p_date-OneDay
    #Get the last VIX value
    while not(PriceExists(VIX_DF,Vix_date)):
        Vix_date-=OneDay
       
    vix_value= VIX_DF.loc[Vix_date]['Close']
    if vix_value<=lf_n:
        lf_adj=1.0
    elif vix_value<lf_s:
        lf_adj= LinInterpolate(lf_n,lf_s,1,0,vix_value)
    else:
        lf_adj=0
    return lf_adj


def LinInterpolate(x0,x1,y0,y1,x):
    return ((y1-y0)/(x1-x0))*(x-x0)

def CppiString():
    if cppi_run:
        return 'CPPI'
    else:
        return ''
def VixModString():
    if vm_flag:
        return 'VM'
    else:
        return ''

def RunForStartRange(a_pc,adtext):
    for start_date in start_rng:
    
        ProjRunID=adtext+CppiString()+VixModString()+str(int(a_pc*100))+'_'+start_date.strftime('%Y%m%d')    
        ac.query_WriteRunDetails((ProjRunID, start_date.date(), number_of_projection_years, float(a_pc), cppi_run,float(floor_pc),float(lev_factor),VixModString(),float(lf_n),float(lf_s)))
        ac.asset.CashBalance=initial_cash_value
        for an_aset in ac.asset.instances:
            an_aset.holding=0.0
            an_aset.value=0.0
            an_aset.div_accrued=0.0

        actual_start=RebalanceForAPeriod(start_date,number_of_projection_years,ProjRunID)


#EXECUTABLE CODE STARTS HERE
# #Adjust run parameters here !!!!!!!!!!!!!!!!!!!!  




#if use_max_rangeis set to True then years_of_start_months is ignored
use_max_range=True
years_of_start_months=1
rebal_flag=True

number_of_projection_years=5

tolerance_pc=0.02
tolerance_ab=250.
initial_cash_value=50000.0

cash_interest=0.01
portfolio_charge=0.0


#cppi parameters

floor_pc=0.90
floor_abs = initial_cash_value* floor_pc

# lev_factor = 4.0

#Scenario Set Up
equity_pc = [0.5]
cppi_flag=[True]
vixmod_flag= [True]

df=pd.DataFrame([equity_pc,cppi_flag,vixmod_flag])
df2=df.transpose()
df2.columns=['equity_pc', 'cppi_flag','vixmod_flag']


vixfile='/Users/jpkemp/Documents/CapPro/Data/VIX.csv'
dateparse = lambda x: pd.datetime.strptime(x, '%Y/%m/%d')
VIX_DF=pd.read_csv(vixfile, parse_dates=['Date'], date_parser=dateparse,usecols=['Date','Close'])
VIX_DF = VIX_DF.set_index(['Date'])

#Set Up Agg
aggprices='/Users/jpkemp/Documents/CapPro/Data/AGG.csv'
aggdist='/Users/jpkemp/Documents/CapPro/Data/AGG Distributions 09042018.csv'
agg=SetUpAsset("BarclaysAgg ETF",'AGG001',aggprices,aggdist)

#Set Up SPY
spyprices = '/Users/jpkemp/Documents/CapPro/Data/SPY.csv'
spydist = '/Users/jpkemp/Documents/CapPro/Data/spy dividends.csv'
spy= SetUpAsset("SPY ETF",'SPY001',spyprices,spydist)


#Get the earliest of the two last dates

last_common_price_date=min(x.pdf.index[-1] for x in ac.asset.instances)
print ('Last Common Price Date: %s'  % last_common_price_date)

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

lf_range=[3.0,5.0]
lf_n=20.0
lf_s=30.0
for lev_factor in lf_range:
    static_addTextRunID = 'mod'+str(round(lev_factor,1))
    for x in df2.index:
        x_pc=df2.iloc[x]['equity_pc']
        cppi_run= df2.iloc[x]['cppi_flag']
        vm_flag= df2.iloc[x]['vixmod_flag']
        spy.target_allocation=x_pc
        agg.target_allocation=1.0-x_pc
        addTextRunID=static_addTextRunID+str(x)
        RunForStartRange(x_pc,addTextRunID)
