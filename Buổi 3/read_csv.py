import pandas as pd
from my_static import MyStatistic

df=pd.read_csv("dataset/SalesTransactions.csv")

my_stat=MyStatistic.find_orders_with_totals(df, 500, 900)

print(my_stat)
