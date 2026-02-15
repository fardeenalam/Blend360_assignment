import duckdb
import time
from pathlib import Path


CSV_FILE = "Amazon Sale Report.csv"

def run_query(con, title, query):
    print("\n" + "=" * 80)
    print(f"{title}")
    print("=" * 80)
    start = time.time()
    try:
        result = con.execute(query).fetchdf()
        print(result)
    except Exception as e:
        print("Error:", e)
    end = time.time()
    print(f"\nExecution Time: {round(end - start, 4)} seconds")


def main():

    csv_path = Path("data\\Amazon Sale Report.csv")
    con = duckdb.connect(database=":memory:")
    table_name = csv_path.stem.lower().replace(" ", "_").replace("-", "_")
    # loading the csv file
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path.resolve()}', header=true)  
        """
    )

#      # 2. Yearly Revenue
#     run_query(
#         con,
#         "Yearly Revenue",
#         f"""
#         SELECT 
#             EXTRACT(YEAR FROM Date) AS year,
#             SUM(Amount) AS yearly_revenue
#         FROM '{CSV_FILE}'
#         GROUP BY year
#         ORDER BY year
#         """
#     )

#     run_query(
#         con,
#         "Year-over-Year Growth",
#         f"""
#         WITH yearly AS (
#             SELECT 
#                 EXTRACT(YEAR FROM Date) AS year,
#                 SUM(Amount) AS revenue
#             FROM '{CSV_FILE}'
#             GROUP BY year
#         )
#         SELECT 
#             year,
#             revenue,
#             LAG(revenue) OVER (ORDER BY year) AS prev_year_revenue,
#             ROUND(
#                 ((revenue - LAG(revenue) OVER (ORDER BY year)) 
#                  / LAG(revenue) OVER (ORDER BY year)) * 100,
#                 2
#             ) AS yoy_growth_percent
#         FROM yearly
#         """
#     )

#     # 4. Top 5 States by Revenue
#     run_query(
#         con,
#         "Top 5 States by Revenue",
#         f"""
#         SELECT 
#             "ship-state" AS state,
#             SUM(Amount) AS total_revenue
#         FROM '{CSV_FILE}'
#         GROUP BY state
#         ORDER BY total_revenue DESC
#         LIMIT 5
#         """
#     )

#     # 5. Revenue by Category
#     run_query(
#         con,
#         "Revenue by Category",
#         f"""
#         SELECT 
#             Category,
#             SUM(Amount) AS total_revenue
#         FROM '{CSV_FILE}'
#         GROUP BY Category
#         ORDER BY total_revenue DESC
#         """
#     )

    # Q1: Underperforming categories in Q4
#     run_query(
#         con,
#         "Lowest Revenue Categories in Q4",
#         f"""
#         SELECT 
#     SUM(Amount) AS total_revenue
# FROM 'Amazon Sale Report.csv'
# WHERE EXTRACT(YEAR FROM Date) = 2022;
#         """
#     )
#     run_query(
#         con,
#         "Lowest Revenue Categories in Q4",
#         f"""
#         SELECT 
#     Category,
#     SUM(Amount) AS total_revenue
# FROM 'Amazon Sale Report.csv'
# GROUP BY Category
# ORDER BY total_revenue DESC
# LIMIT 1;
#         """
#     )
#     run_query(
#         con,
#         "Lowest Revenue Categories in Q4",
#         f"""
#         SELECT 
#     ROUND(
#         SUM(CASE WHEN Status ILIKE '%Cancelled%' THEN 1 ELSE 0 END) * 100.0
#         / COUNT(*),
#         2
#     ) AS cancellation_rate_percent
# FROM 'Amazon Sale Report.csv';
#         """
#     )
    run_query(
        con,
        "performing",
        f"""
        SELECT "Category", SUM("Amount") AS "total_amount"
FROM amazon_sale_report
GROUP BY "Category"
ORDER BY "total_amount" DESC
LIMIT 1;
        """
    )

    # run_query(con, "total amount",f"""
    # SELECT
    #   SUM("Amount") AS "total_revenue"
    # FROM amazon_sale_report;
    # """)






if __name__ == "__main__":
    main()