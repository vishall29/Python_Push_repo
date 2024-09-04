import pandas as pd
import xlrd
import xlrd.sheet
df=pd.read_excel(r'C:\Users\visha\Downloads\Sample - Superstore.xls')
print(df.head())
df['Price'] = df['Sales'] / df['Quantity']
print(df.head())
import openpyxl
state_code=pd.read_excel(r'C:\Users\visha\Downloads\abberrevation.xlsx')
print(state_code.head())
Project_final = pd.merge(df, state_code, on='State', how='left')
print(Project_final.head())
Project_final.to_csv(r'C:\Users\visha\Downloads\Vishal_Task3.csv', index=False)
df.head()
df.info()
df2=df.nlargest(3,"Price")
print(df2.head())
bottom_3_price =df.nsmallest(3,"Price")
print(bottom_3_price.head())
df2.to_csv(r'C:\Users\visha\Downloads\top3_price.csv', index=False)
bottom_3_price.to_csv(r'C:\Users\visha\Downloads\bottom3_price.csv', index=False)
sales_by_state=df.groupby('State')['Sales'].sum()
print(sales_by_state.head())
sales_by_state.to_csv(r'C:\Users\visha\Downloads\sales_by_state3.csv', index=True)
print(sales_by_state.head())
sales_by_Customer=df.groupby('Customer Name')['Sales'].sum()
sales_by_Customer2=sales_by_Customer.head()
sales_by_Customer2.to_csv(r'C:\Users\visha\Downloads\sales_by_customer.csv', index=True)
