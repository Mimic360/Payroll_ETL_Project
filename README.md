# Payroll ETL Project (Python)

This project tries to simulates a real-world ETL (Extract, Transform, Load) process for payroll data using Python, Pandas, and SQLite. It demonstrates data cleaning, batch processing, SQL validation, and data exports with error handling.

## Project Structure

payroll_automation.py # Main ETL pipeline script
data/ # Raw CSV input files
outputs/ # Cleaned reports and visualizations
payroll_data.db # SQLite database storing processed data

## Technologies Used

- Python 3.x  
- Pandas  
- SQLite  
- SQL queries  
- Matplotlib (visualizations)  
- Logging for traceability  

## Features

Batch processing of multiple CSV files  
Data cleaning and payroll calculations  
Department-level summaries  
SQL-based data validation  
Export of clean reports and visualizations  
Modular, production-style ETL structure  


## How to Run

1. Place your raw payroll `.csv` files inside the `data/` folder  
2. Run the script:  
   ```bash
   python payroll_automation.py data
