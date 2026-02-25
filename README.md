# Project Overview

This project analyzes a retail company's consumer shopping behavior dataset containing 3,900+ customer records across attributes such as age, gender, product category, purchase amount, season, payment method, discount usage, and purchase frequency. The primary goal is to uncover actionable insights that help the business improve sales performance, customer engagement, and long-term loyalty.

## Project Workflow
The project follows a structured end-to-end data pipeline. First, **Data Preprocessing** was performed using Python (Pandas, NumPy) to clean the raw dataset — handling missing values, standardizing column formats, encoding categorical variables, and ensuring data consistency across all fields. Once the data was clean and structured, it was **loaded into a PostgreSQL database**, where tables were designed to efficiently store and query customer, product, and transaction-level information, enabling reliable and scalable data management. Finally, an **interactive dashboard** was built on top of the PostgreSQL data to visually explore key trends — including sales by category and season, the impact of discounts and promo codes, customer demographics, payment preferences, and repeat purchase behavior — giving the management team a clear, data-driven view of what drives consumer decisions.

## How to Run
1. Run the Python preprocessing script to clean the raw CSV data.
2. Connect to your PostgreSQL instance and execute the SQL schema + insert scripts to load the cleaned data.
3. Connect your dashboard tool to the PostgreSQL database and open the dashboard file to explore the visualizations.
