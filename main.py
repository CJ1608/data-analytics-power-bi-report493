from db_utils import RDSDatabaseConnector
from data_extraction import DataExtractor
import pandas as pd

if __name__ == '__main__':
    
    #  Get credentials and connect to database
     db_connector = RDSDatabaseConnector('credentials.yaml')
     db_connector.read_db_creds()
     db_connector.init_db_engine()

     # print list of the tables in orders table
     table_names = db_connector.list_db_tables()
    #  print(table_names)
     
    # print all of tables to csv for later use
     db_extractor = DataExtractor()
    #  for element in table_names:
    #     df_extracted= db_extractor.read_rds_table(db_connector, element)
    #     df_extracted.to_csv(f'{element}.csv')
        
    # Find how many staff there are in all of the UK stores
     UK_staff = pd.read_sql("SELECT SUM(staff_numbers) FROM dim_store ", con=db_connector.init_db_engine())
     print(UK_staff)

    # Find which month in 2022 had the highest revenue
     highest_revenue_month = pd.read_sql("SELECT ROUND(CAST(SUM((dim_product.sale_price - dim_product.cost_price) * orders.product_quantity) AS NUMERIC), 2) as total_revenue, dim_date.month_name\
                                         FROM dim_product\
                                        INNER JOIN orders\
                                            ON dim_product.product_code = orders.product_code\
                                        INNER JOIN dim_date\
                                            ON orders.order_date = dim_date.date\
                                        WHERE dim_date.year = 2022\
                                        GROUP BY dim_date.month_name\
                                        ORDER BY total_revenue DESC\
                                        LIMIT 1",  con=db_connector.init_db_engine())
     print(highest_revenue_month)

    # Find which German store type had the highest revenue for 2022
     highest_revenue_german_2022 = pd.read_sql("SELECT ROUND(CAST(SUM((dim_product.sale_price - dim_product.cost_price) * orders.product_quantity) AS NUMERIC), 2) as total_revenue, dim_store.store_code\
                                        FROM dim_product\
                                        INNER JOIN orders\
                                            ON dim_product.product_code = orders.product_code\
                                        INNER JOIN dim_date\
                                            ON orders.order_date = dim_date.date\
                                        INNER JOIN dim_store\
                                               ON orders.store_code = dim_store.store_code\
                                        WHERE dim_date.year = 2022 AND dim_store.country_code = 'DE'\
                                        GROUP BY dim_store.store_code\
                                        ORDER BY total_revenue DESC\
                                        LIMIT 1",  con=db_connector.init_db_engine())
     print(highest_revenue_german_2022)

    # Create a view where the rows are the store types and the columns are the total sales, percentage of total sales and the count of orders
     sales_and_orders = pd.read_sql("CREATE VIEW Sales_and_Orders AS\
                                    SELECT dim_store.store_type as store_type,\
                                        ROUND(CAST(SUM(orders.product_quantity * dim_product.sale_price) AS numeric), 2) AS total_sales,\
	                                    ROUND(SUM(100 * orders.product_quantity*dim_product.sale_price)/(SUM(SUM(orders.product_quantity*dim_product.sale_price)) OVER ())) as percentage_total,\
                                        COUNT(orders.order_date_uuid) as count_of_orders\
                                    FROM dim_product\
                                    INNER JOIN orders\
                                        ON orders.product_code = dim_product.product_code\
                                    INNER JOIN dim_store\
                                        ON orders.store_code = dim_store.store_code\
                                    GROUP BY dim_store.store_type;\
                                    SELECT * FROM Sales_and_Orders",  con=db_connector.init_db_engine())
     print(sales_and_orders)

    # Find which product category generated the most profit for the "Wiltshire, UK" region in 2021
     most_profitable_category_Wiltshire_2021 = pd.read_sql("SELECT ROUND(CAST(SUM((dim_product.sale_price - dim_product.cost_price) * orders.product_quantity) AS NUMERIC), 2) as total_profit,\
                                                          dim_product.category\
                                                          FROM dim_product\
                                                        INNER JOIN orders\
                                                            ON dim_product.product_code = orders.product_code\
                                                        INNER JOIN dim_date\
                                                            ON orders.order_date = dim_date.date\
                                                        INNER JOIN dim_store\
                                                            ON orders.store_code = dim_store.store_code\
                                                        WHERE dim_date.year = 2021 AND dim_store.full_region = 'Wiltshire, UK'\
                                                        GROUP BY dim_product.category\
                                                        ORDER BY total_profit DESC\
                                                        LIMIT 1", con=db_connector.init_db_engine())
     print(most_profitable_category_Wiltshire_2021)

    # Generate .csv files of the query results 
     queries_to_print = [UK_staff, highest_revenue_month, highest_revenue_german_2022, sales_and_orders, most_profitable_category_Wiltshire_2021]
     counter = 1
     for element in queries_to_print:
        element.to_csv(f'question{counter}.csv')
        counter += 1