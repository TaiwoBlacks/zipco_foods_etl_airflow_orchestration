import pandas as pd

def run_transformation():
data = pd.read_csv("rawdata/ZipcoFoods_RawData.csv")

# Remove duplicates
data = data.drop_duplicates()

# Handle missing values for numeric columns
numeric_cols = data.select_dtypes(include=['float64', 'int64']).columns
for col in numeric_cols:
    data[col].fillna(data[col].mean(), inplace=True)

# Handling missing values for string columns
string_cols = data.select_dtypes(include=['object']).columns
for col in string_cols:
    data[col].fillna(('unknown'), inplace=True)
    
# Change date column to datetime format
data['Date'] = pd.to_datetime(data['Date'])

# Creating fact and dimension tables
# Product table
product = data[['ProductName', 'UnitPrice']].drop_duplicates().reset_index(drop=True)
product.index.name = 'ProductID'
product = product.reset_index() 

# Customer table
customer = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
customer.index.name = 'CustomerID'
customer = customer.reset_index()

# Staff table
staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
staff.index.name = 'StaffID'
staff = staff.reset_index()

# Transaction table
# Transactions table
# Transactions table
transaction = data.merge(product, on=['ProductName', 'UnitPrice'], how='left') \
    .merge(customer, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left') \
    .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left') \

transaction.index.name = 'TransactionID'
transaction = transaction.reset_index() \
                            [['Date', 'TransactionID', 'ProductID', 'UnitPrice', 'Quantity', 'StoreLocation', 'PaymentType', \
                                'PromotionApplied', 'Weather', 'Temperature', 'StaffPerformanceRating', 'CustomerFeedback', \
                                'DeliveryTime_min', 'OrderType', 'CustomerID', 'StaffID', 'DayOfWeek', 'TotalSales']]
                            
# Save the tables as CSV files
data.to_csv("cleandata/cleandata.csv", index=False)
product.to_csv("cleandata/product.csv", index=False)
customer.to_csv("cleandata/customer.csv", index=False)
staff.to_csv("cleandata/staff.csv", index=False)
transaction.to_csv("cleandata/transaction.csv", index=False)

print("Data cleaning and transformation completed successfully")
