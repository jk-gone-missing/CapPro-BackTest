import weakref
#import mysql.connector
import pymysql

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

def query_WriteRunDetails(data):
    cnx = databaseconnector()
    cursor = cnx.cursor()
    query=("INSERT INTO RunDetails "
               "(RunID, StartDate, Years, EquityPC, CPPIFlag,CPPIFloor,CPPILevFactor,CPPIVixMod,Mod_N,Mod_S) "
               "VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s,%s)")
    cursor.execute(query,data)
    cnx.commit()
    cursor.close
    cnx.close

def query_transaction(data):
    cnx = databaseconnector()
    cursor = cnx.cursor()
    query=("INSERT INTO transactions "
               "(RunID, tDate, InvestmentID, tType, Price,tShares,tValue,tCosts) "
               "VALUES (%s, %s, %s, %s, %s, %s,%s,%s)")
    cursor.execute(query,data)
    cnx.commit()
    cursor.close
    cnx.close

def query_balance(data):
    cnx = databaseconnector()
    cursor = cnx.cursor()
    query=("INSERT INTO balances "
               "(RunID, bDate, InvestmentID, Price,bShares,bValue,Proportion,tProp) "
               "VALUES (%s, %s, %s, %s, %s, %s,%s,%s)")
    cursor.execute(query,data)
    cnx.commit()
    cursor.close
    cnx.close

def WriteBalances(RunID,bal_date):
    #Writes the balance of each instance of asset and then writes the cash balance as well.
    total_value =GetPortfolioValue()
    for an_asset in asset.instances:
        an_value = round(float(an_asset.holding*an_asset.price),2)
        qdata=(RunID,bal_date.date(),an_asset.InvestmentID,float(an_asset.price),float(an_asset.holding),an_value,float(an_value/total_value),float(an_asset.target_allocation))
        query_balance(qdata)
        an_div=float(an_asset.div_accrued)
        if an_div>0.0:
            qdata=(RunID,bal_date.date(),an_asset.InvestmentID,0.0,0.0,an_div,float(an_div/total_value),0.0)
            query_balance(qdata)
            
    qdata=(RunID,bal_date.date(),'CashBal',float(0.0),float(0.0),float(asset.CashBalance),float(asset.CashBalance/total_value),0.0)
    query_balance(qdata)

def GetPortfolioValue():
    total=0.0
    for x in asset.instances:
        total+=x.value+x.div_accrued
    total+=asset.CashBalance
    return total

def CleanTestData():
    cnx = databaseconnector()
    #cnx = mysql.connector.connect(user='root', password='actuary',
    #                            host='127.0.0.1',
    #                            database='cppi_test')
    
    cursor = cnx.cursor()
    #delquery=("DELETE FROM balances WHERE  RunID = (%s)") % delRunID
    delquery="DELETE FROM balances"
    cursor.execute(delquery)
    #delquery=("DELETE FROM transactions WHERE RunID = (%s)") % delRunID
    delquery="DELETE FROM transactions"
    cursor.execute(delquery)
    cnx.commit()
    cursor.close
    cnx.close
    
def TransactionCosts(tranValue,tranShares):
    baseCost=0.005*tranShares
    baseCost=min(baseCost,tranValue*0.005)
    baseCost=max(baseCost,1.00)
    return float(baseCost)

class asset:
    'Common base class for all assets'
    CashBalance = 0
    instances = []
   
    def __init__(self, name, InvestmentID, holding,target_allocation,price,value,div_accrued,pdf,ddf):
      self.name = name
      self.InvestmentID=InvestmentID
      self.holding = holding
      self.target_allocation = target_allocation
      self.price=price
      self.value= value
      self.div_accrued=div_accrued
      self.pdf=pdf
      self.ddf=ddf
      self.__class__.instances.append(weakref.proxy(self))
      
   
    def buy(self,RunID,tran_date,buy_value):
        thistransaction=int(buy_value/self.price)
        thistransaction_value = round(thistransaction*self.price,2)
        if thistransaction>=1:
            #the above checks that at least one share can be bought
            self.holding +=thistransaction
            self.value +=thistransaction_value
            t_cost=TransactionCosts(thistransaction_value,thistransaction)
            asset.CashBalance-=(thistransaction_value+t_cost)
            query_data= (RunID,tran_date.date(),self.InvestmentID,'BUY',float(self.price),float(thistransaction),float(thistransaction_value),t_cost)
            query_transaction(query_data)

    def sell(self,RunID,tran_date,sell_quant):
        thistransaction_value=round(self.price*sell_quant,2)
        self.holding-=sell_quant
        t_cost=TransactionCosts(thistransaction_value,sell_quant)
        asset.CashBalance+=(sell_quant*self.price-t_cost)
        query_data= (RunID,tran_date.date(),self.InvestmentID,'SELL',float(self.price),float(sell_quant),float(thistransaction_value),t_cost)
        query_transaction(query_data)

       
    def sell_est_value(self,RunID,tran_date,sell_value):
        if sell_value>self.price:#ensures that at least 1 share can be sold.
            thistransaction=int(sell_value/self.price)
            thistransaction_value=round(thistransaction*self.price,2)
            t_cost=TransactionCosts(thistransaction_value,thistransaction)
            self.holding-=thistransaction
            self.value -=thistransaction_value
            asset.CashBalance+=(thistransaction_value-t_cost)
            query_data= (RunID,tran_date.date(),self.InvestmentID,'SELL',float(self.price),float(thistransaction),float(thistransaction_value),t_cost)
            query_transaction(query_data)




     