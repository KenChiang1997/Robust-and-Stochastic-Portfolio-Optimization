import numpy as np 
import pandas as pd 
import yfinance as yf 
import datetime as dt

import warnings
warnings.filterwarnings('ignore')


def daily_investment_value(shares,price):
    return shares * price 


def single_asset_back_testing(price_df,initial_investment,investment_weight,price_column_name):
    """
    """
    investment_value = initial_investment * investment_weight

    price_df['Asset Retrun'] = price_df[[str(price_column_name)]].pct_change()
    price_df['#Share'] = int(investment_value/price_df.iloc[0][str(price_column_name)])

    price_df['Investment Value'] = price_df.apply(lambda x: daily_investment_value(shares=x['# Share'], price=x[str(price_column_name)]),axis=1)
    price_df['Remain Cash'] = investment_value - price_df['# Share'].iloc[0] * price_df[price_column_name].iloc[0]
    price_df['PnL'] = price_df['Investment Value'].diff()
    price_df['To Date. Return'] = (price_df['Investment Value'] / price_df.iloc[0]['Investment Value'])

    return price_df


def multi_asset_back_testing(price_df,initial_investment,investment_weights,price_column_name):
    """
    """
    
    investment_values = [ weight * initial_investment for weight in investment_weights ]

    price_df[[str(tick) + " return" for tick in price_column_name]] = price_df[[str(tick) for tick in price_column_name]].pct_change()
    price_df[[str(tick) + " #Share" for tick in price_column_name]]= [ int(invesment/price_df[tick].iloc[0]) for (invesment,tick) in zip(investment_values,price_column_name)]

    for tick in price_column_name:        
       price_df[str(tick) + " Investment Value"] = price_df.apply(lambda x: daily_investment_value(shares=x[str(tick) + " #Share" ], price=x[str(tick)]),axis=1)

    price_df['Total Investment Value'] = price_df[[str(tick) + " Investment Value" for tick in price_column_name]].sum(axis=1)
    price_df['Investment PnL'] = price_df['Total Investment Value'] - price_df['Total Investment Value'].iloc[0]
    price_df['Remain Cash'] = initial_investment - price_df['Total Investment Value'].iloc[0]
    price_df['Total Value'] = price_df['Total Investment Value'] + price_df['Remain Cash']

    price_df['Investment To Date. Return(%)'] = ((price_df['Total Investment Value'] / price_df.iloc[0]['Total Investment Value']) -1) *100


    return price_df

def multi_asset_back_testing(price_df,initial_investment,investment_weights,price_column_name):
    """
    """
    
    investment_values = [ weight * initial_investment for weight in investment_weights ]

    price_df[[str(tick) + " return" for tick in price_column_name]] = price_df[[str(tick) for tick in price_column_name]].pct_change()
    price_df[[str(tick) + " #Share" for tick in price_column_name]]= [ int(invesment/price_df[tick].iloc[0]) for (invesment,tick) in zip(investment_values,price_column_name)]

    for tick in price_column_name:        
       price_df[str(tick) + " Investment Value"] = price_df.apply(lambda x: daily_investment_value(shares=x[str(tick) + " #Share" ], price=x[str(tick)]),axis=1)

    price_df['Total Investment Value'] = price_df[[str(tick) + " Investment Value" for tick in price_column_name]].sum(axis=1)
    price_df['Investment PnL'] = price_df['Total Investment Value'] - price_df['Total Investment Value'].iloc[0]
    price_df['Remain Cash'] = initial_investment - price_df['Total Investment Value'].iloc[0]
    price_df['Total Value'] = price_df['Total Investment Value'] + price_df['Remain Cash']

    price_df['Investment To Date. Return(%)'] = ((price_df['Total Investment Value'] / price_df.iloc[0]['Total Investment Value']) -1) *100

    return price_df


def Monthly_Rebalance(Stock_DF,price_column_name,investment_weights,initial_investment):
    """
    Construct { Investment PnL, Remain Cash, Total Value, Investment To Date. Return(%), Investment PnL Change }
    But, we still have to adjust the Profit Transaction Date (ex. 10/31 - 11/01) with another function.
    """
    back_testing_df = pd.DataFrame()
    
    for i,(name,df) in enumerate(Stock_DF.to_period('M').groupby('Date')):
        
        investment_weight = investment_weights[i]
        period_back_testing_df = multi_asset_back_testing(df,initial_investment,investment_weight,price_column_name)

        back_testing_df = pd.concat([back_testing_df, period_back_testing_df],axis=0)
        back_testing_df[[str(tick) + " return" for tick in price_column_name]] = back_testing_df[[str(tick) for tick in price_column_name]].pct_change()
        back_testing_df['Investment PnL'] = back_testing_df['Total Investment Value'] - back_testing_df['Total Investment Value'].iloc[0]
        back_testing_df['Investment PnL Change'] = back_testing_df['Investment PnL'].diff().shift(-1)
        initial_investment = back_testing_df['Total Investment Value'].iloc[-1] + back_testing_df['Remain Cash'].iloc[-1]

    back_testing_df.index = Stock_DF.index
    
    return back_testing_df

def Adjust_Backtesting_Result(back_testing_df,price_column_name,investment_weights,initial_investment):
    """
    # Construct { Investment PnL, Remain Cash, Total Value, Investment To Date. Return(%), Investment PnL Change }
    """
    all_back_testing_df = pd.DataFrame()

    for i,(name,df) in enumerate(back_testing_df.to_period('M').groupby('Date')):

        price_df = df 
        investment_weight = investment_weights[i]

        # period backtesting with share and index investment value
        period_back_testing_df = multi_asset_back_testing(price_df,initial_investment,investment_weight,price_column_name)
        all_back_testing_df = pd.concat([all_back_testing_df, period_back_testing_df],axis=0)

        # Construct { Investment PnL, Remain Cash, Total Value, Investment To Date. Return(%), Investment PnL Change }
        all_back_testing_df[[str(tick) + " return" for tick in price_column_name]] = all_back_testing_df[[str(tick) for tick in price_column_name]].pct_change()
        all_back_testing_df['Investment PnL'] = all_back_testing_df['Total Investment Value'] - all_back_testing_df['Total Investment Value'].iloc[0]
        all_back_testing_df['Investment PnL Change'] = all_back_testing_df['Investment PnL'].diff().shift(-1)
        all_back_testing_df['Investment To Date. Return(%)'] = all_back_testing_df['Total Investment Value'] / all_back_testing_df['Total Investment Value'].iloc[0]

        # reset initial investment value for evey month periods
        initial_investment = period_back_testing_df['Total Investment Value'].iloc[-1] + period_back_testing_df['Remain Cash'].iloc[-1] - period_back_testing_df['Investment PnL Change'].iloc[-1]

    # Split the backtesting result to 3 main table(asset price and return, asset investment share and value, asset backtesting result)
    all_back_testing_df.index = back_testing_df.index
    investment_asset_columns = list([str(tick) for tick in price_column_name])
    investment_asset_columns.extend([str(tick) + " return" for tick in price_column_name])
    investment_asset_share =  list([str(tick) + " #Share" for tick in price_column_name])
    investment_asset_share.extend([str(tick) + " Investment Value" for tick in price_column_name])
    investment_asset_share.extend(['Total Investment Value'])
    investment_asset_share.extend(['Remain Cash'])
    investment_asset_share.extend(['Total Value'])

    investment_asset_result =  all_back_testing_df[investment_asset_columns]
    investment_share_value = all_back_testing_df[investment_asset_share]
    back_testing_result = all_back_testing_df[['Total Investment Value', 'Investment PnL','Remain Cash', 'Total Value', 'Investment To Date. Return(%)']]
    
    return investment_asset_result, investment_share_value, back_testing_result


def main():

    # Portfolio : 10 Assets
    start_date = '2022-10-27'
    end_date   = '2022-11-04'

    # yfinance likes the tickers formatted as a list
    tickers = ["MSFT","AAPL","AMZN"]
    ticks     = yf.Tickers(tickers)
    Stock_DF  = ticks.history(start=start_date, end=end_date).Close[tickers] # make sure we still have same ticker order 
    price_column_name = Stock_DF.columns

    # Set Backtesting Parameters 
    initial_investment = 1300
    investment_weight_1 = [0.5,0.5,0]
    investment_weight_2 = [0.1,0.4,0.5]
    investment_weights = []
    investment_weights.append(investment_weight_1)
    investment_weights.append(investment_weight_2)

    print("Initial Investment: ",initial_investment,"\n")
    print("Trading Asset: ",price_column_name,"\n")
    
    back_testing_df = Monthly_Rebalance(Stock_DF,price_column_name,investment_weights,initial_investment)
    investment_asset_result, investment_share_value, back_testing_result = Adjust_Backtesting_Result(back_testing_df,price_column_name,investment_weights,initial_investment)

    print("Investment Asset's Price and  Return:\n", investment_asset_result,"\n")
    print("Investment Share and Value:\n", investment_share_value,"\n")
    print("Backtesting Result:\n", back_testing_result,"\n")

if __name__ == "__main__":
    main()