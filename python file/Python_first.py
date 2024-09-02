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
Project_final.to_csv(r'C:\Users\visha\Downloads\Vishal_Task1.csv', index=False)