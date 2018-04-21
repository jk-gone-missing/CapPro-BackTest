import pandas as pd
import numpy as np
import pandas_datareader as pdr
import seaborn as sns
from matplotlib import pyplot as plt
# not needed, only to prettify the plots.
import matplotlib
from IPython.display import set_matplotlib_formats
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

# prettify the figures
plt.style.use(['seaborn-white', 'seaborn-paper'])
matplotlib.rc('font', family='Times New Roman', size=15)
set_matplotlib_formats('png', 'png', quality=90)
plt.rcParams['savefig.dpi'] = 150
plt.rcParams['figure.autolayout'] = False
plt.rcParams['figure.figsize'] = 8, 5
plt.rcParams['axes.labelsize'] = 10
plt.rcParams['axes.titlesize'] = 15
plt.rcParams['font.size'] = 12
plt.rcParams['lines.linewidth'] = 1.0
plt.rcParams['lines.markersize'] = 8
plt.rcParams['legend.fontsize'] = 12
plt.rcParams['ytick.labelsize'] = 11
plt.rcParams['xtick.labelsize'] = 11
plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.serif'] = 'cm'
plt.rcParams['axes.grid'] = True

kw_save = dict(bbox_iches='tight', transparent=True)
# asset information
asset_info = '''
Comparison SPY vs AGG
'''
# useful functions
# ================
def total_return(prices):
    """Retuns the return between the first and last value of the DataFrame.

    Parameters
    ----------
    prices : pandas.Series or pandas.DataFrame

    Returns
    -------
    total_return : float or pandas.Series
        Depending on the input passed returns a float or a pandas.Series.
    """
    return prices.iloc[-1] / prices.iloc[0] - 1


def total_return_from_returns(returns):
    """Retuns the return between the first and last value of the DataFrame.

    Parameters
    ----------
    returns : pandas.Series or pandas.DataFrame

    Returns
    -------
    total_return : float or pandas.Series
        Depending on the input passed returns a float or a pandas.Series.
    """
    return (returns + 1).prod() - 1



def plot_this(df, df2,title, figsize=None, ylabel='',
             output_file='imgs/fig.png', bottom_adj=0.25,
             txt_ymin=-0.4, bar=1):
    if bar==0:
        ax = df.plot.bar(title=title, figsize=figsize)
    elif bar==2:
        ax = df.plot(title=title, figsize=figsize)
        bx = df2.plot(title=title, figsize=figsize)
    else:
        ax = df.plot(title=title, figsize=figsize) 
    sns.despine()
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.text(0, txt_ymin, asset_info, transform=ax.transAxes, fontsize=9)
    plt.gcf().subplots_adjust(bottom=bottom_adj)
    plt.savefig(output_file, **kw_save)


# Geting data
# ===========
today = '20170926'  # to make static this script.
tckr = 'SPY'  # Banco do Brasil SA
# download data

spyprices = '/Users/jpkemp/Documents/CapPro/Data/SPY.csv'
aggprices='/Users/jpkemp/Documents/CapPro/Data/AGG.csv'
dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
dateparse2 = lambda x: pd.datetime.strptime(x, '%d/%m/%Y')
data=pd.read_csv(spyprices, parse_dates=['Date'],date_parser=dateparse2)
AGG_Price_DF=pd.read_csv(aggprices, parse_dates=['Date'], date_parser=dateparse)
data.columns = data.columns.map(lambda col: col.lower())
AGG_Price_DF.columns = AGG_Price_DF.columns.map(lambda col: col.lower())
data = data.set_index(['date'])
AGG_Price_DF = AGG_Price_DF.set_index(['date'])





# using close prices
spy_prices = data.close.copy()
agg_prices = AGG_Price_DF.close.copy()
spy_prices=spy_prices.to_frame()
agg_prices=agg_prices.to_frame()
# print (type(spy_prices))
# print(spy_prices.head())
start_date='2004-12-31'
start_date=pd.datetime.strptime(start_date, '%Y-%m-%d')
end_date='2009-12-31'
end_date=pd.datetime.strptime(end_date, '%Y-%m-%d')
# we convert to DataFrame to make easy store more series.
#results_storage = spy_prices.to_frame().copy()

rebal=SelectQuery('select bDate as date, Total as close from TotalBalance  where RunID="test_20041231"',True)

rebal_df=pd.DataFrame(list(rebal), columns=['date', 'close'])
rebal_df = rebal_df.set_index(['date'])
rebal_start_price=rebal_df.iloc[0]['close']
rebal_df.close=rebal_df.close/rebal_start_price*100

mask = (spy_prices.index >= start_date) & (spy_prices.index <= end_date)
mask2 = (agg_prices.index >= start_date) & (agg_prices.index <= end_date)
spy_range=spy_prices.loc[mask]
agg_range = agg_prices.loc[mask2]

spy_start_price = spy_range.iloc[0]['close']
spy_range.close=spy_range.close/spy_start_price*100


agg_start_price = agg_range.iloc[0]['close']
agg_range.close=agg_range.close/agg_start_price*100

# plot_this(spy_range, agg_range,title='Prices of %s' % tckr, ylabel='Prices in USD',
#           txt_ymin=-0.2, bottom_adj=0.15, output_file='/Users/jpkemp/Documents/CapPro/fig_prices.png',bar=2)
plt.plot(spy_range)
plt.plot(agg_range)
plt.plot(rebal_df)
plt.legend(['SPY', 'AGG', '50/50 Rebal'], loc='upper left')
output_file='/Users/jpkemp/Documents/CapPro/fig_prices.png'
plt.savefig(output_file, **kw_save)