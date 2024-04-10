import sqlite3
import pandas as pd

class DatabaseManager:
    def __init__(self,db_name):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect1(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
    def execute_total_quanty(self):
        #extract the total quantities of each item bought per customer aged 18-35
        query1='''select c.customer_id,c.age,i.item_name,SUM(o.quantity) as Total_quantity from Sales s join customers c on s.customer_id = c.customer_id
        join orders o on s.sales_id = o.sales_id Join items i on o.item_id = i.item_id
        where c.age>= 18 and c.age<= 35
        group by i.item_name,c.customer_id,c.age
        ;'''
        #results = self.cur_obj.execute(query1)
        #res = results.fetchall() 
        df = pd.read_sql_query(query1, self.conn)
        # Replace NA values with 0 in 'Total_quantity' column
        df['Total_quantity'] = df['Total_quantity'].fillna(0)
        # Convert 'Total_quantity' column to integers
        df['Total_quantity'] = df['Total_quantity'].astype(int)
        #items with no purchase(total_quantity=0) should be omitted to the final list
        df = df[df['Total_quantity'] != 0]
        #print(df)
        # Save the DataFrame to a CSV file with semicolon delimiter
        df.to_csv('query1_result.csv', sep=';', index=False)
        print("CSV file for query1 dataframe has been created successfully.")

        ##for each customer,get the sum of each item
        query2='''select c.customer_id,c.age,i.item_name,sum(o.quantity) as Total_quantity from Sales s join customers c on s.customer_id = c.customer_id
        join orders o on s.sales_id = o.sales_id Join items i on o.item_id = i.item_id
        group by i.item_name,c.customer_id ;'''
        df2 = pd.read_sql_query(query2, self.conn)
        df2['Total_quantity'] = df2['Total_quantity'].fillna(0)
        # Convert 'Total_quantity' column to integers
        df2['Total_quantity'] = df2['Total_quantity'].astype(int)
        #items with no purchase(total_quantity=0) should be omitted to the final list
        df2 = df2[df2['Total_quantity'] != 0]
        #print(df2)
        # Save the DataFrame to a CSV file with semicolon delimiter
        df2.to_csv('query2_result.csv', sep=';', index=False)

        print("CSV file for query2 dataframe has been created successfully.")
        

# Example usage:
if __name__ == "__main__":
    db_manager = DatabaseManager('Data Engineer_ETL Assignment.db')
    print(db_manager)
    db_manager.connect1()
    db_manager.execute_total_quanty()



