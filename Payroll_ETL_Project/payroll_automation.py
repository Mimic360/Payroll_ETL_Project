import pandas as pd
import sqlite3
import logging
import sys 
from datetime import datetime
import os
import xlsxwriter
import matplotlib.pyplot as plt

# Get the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, 'payroll_data.db')

def print_file_locations():
    """Print information about where files are saved"""
    print(f"\n{'='*60}")
    print("FILE LOCATIONS:")
    print(f"Script Directory: {SCRIPT_DIR}")
    print(f"Database: {DB_PATH}")
    print(f"Excel Files: {SCRIPT_DIR}")
    print(f"Export Folders: {SCRIPT_DIR}/payroll_exports_[timestamp]")
    print(f"{'='*60}\n")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def extract(file_path):
    logging.info(f"Extracting data from {file_path}...")
    # Handle both CSV and Excel files
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith(('.xlsx', '.xls')):
        return pd.read_excel(file_path)
    else:
        logging.error(f"Unsupported file format: {file_path}")
        return None


def transform(df):
    logging.info("Transforming data...")
    
    # Check if the DataFrame is empty
    if df is None or df.empty:
        logging.warning("Input DataFrame is empty.")
        return None

    # Check if the required columns are present
    required_cols = ['Emp ID', 'Emp Name', 'Department', 'Hourly Rate', 'Hours Worked', 'Pay Date', 'Notes']
    for col in required_cols:
        if col not in df.columns:
            logging.error(f"Missing required column: {col}")
            return None
    logging.info("All required columns are present.")

    # Check for missing columns
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logging.error(f"Missing columns in input file: {', '.join(missing_cols)}")
        return None

    # Striping the whitespace and standardize text columns
    df['Emp Name'] = df['Emp Name'].str.strip().str.title()
    df['Department'] = df['Department'].str.strip().str.title()
    df['Notes'] = df['Notes'].fillna('').str.strip()
    df['Pay Date'] = pd.to_datetime(df['Pay Date'])

    # Drop rows with missing or invalid dates
    df = df.dropna(subset=['Pay Date'])

    df = df[(df['Hourly Rate'] > 0) & (df['Hours Worked'] > 0)]

    # Calculate tax based on department
    tax_rates = {
        'It' : 0.15,
        'Hr' : 0.12,
        'Finance' : 0.14,
        'Sales' : 0.16,
        'Marketing' : 0.13
    }
    
    # calculate Gross Pay, Net Pay, and flag hours worked
    df['Gross Pay'] = df['Hourly Rate'] * df['Hours Worked']
    df['Hours Flag'] = df['Hours Worked'].apply(lambda x: 'Overtime' if x > 40 else 'Regular')

    # Apply tax rates based on department
    df['Tax Rate'] = df['Department'].map(tax_rates).fillna(0.10)  # Default tax rate if department not found
    df['Tax'] = df['Gross Pay'] * df['Tax Rate']
    df['Net Pay'] = df['Gross Pay'] - df['Tax']

    # New columns for reporting overtime worked
    df['Overtime Hours'] = df['Hours Worked'].apply(lambda x: x - 40 if x > 40 else 0)
    df['Regular Hours'] = df['Hours Worked'].apply(lambda x: 40 if x > 40 else x)

    # Group by department and calculate summary statistics
    dept_summary = df.groupby('Department').agg({
        'Gross Pay': 'sum',
        'Tax': 'sum',
        'Net Pay': 'sum',
        'Emp ID': 'count'
    }).reset_index()

    # Rename columns 
    dept_summary.rename(columns={
        'Emp ID': 'Employee Count'
    }, inplace=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Check for hours worked over 40 and flag them
    warnings = df[df['Hours Flag'] == 'Overtime']

    # Export Results to script directory
    df.to_excel(os.path.join(SCRIPT_DIR, f'cleaned_processed_payroll_{timestamp}.xlsx'), index=False, engine='xlsxwriter')
    dept_summary.to_excel(os.path.join(SCRIPT_DIR, f'department_summary_{timestamp}.xlsx'), index=False, engine='xlsxwriter')
    warnings.to_excel(os.path.join(SCRIPT_DIR, f'hours_warning_report_{timestamp}.xlsx'), index=False, engine='xlsxwriter')
    
    print(f"\nExcel files exported successfully ({timestamp})!")
    logging.info(f"Data successfully transformed and exported to Excel files")
        
    return df, dept_summary, warnings
    

def load(df, dept_summary, warnings):
    logging.info("Loading data into SQLite database...")
    conn = sqlite3.connect(DB_PATH)
    df.to_sql('payroll_records', conn, if_exists='append', index=False)
    dept_summary.to_sql('department_summary', conn, if_exists='append', index=False)
    warnings.to_sql('overtime_warnings', conn, if_exists='append', index=False)
    conn.close()
    logging.info("Data successfully loaded into SQLite database")

def validate_load():
    logging.info("Validating data load...")
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT Department, SUM([Net Pay]) as Total_Net_Pay FROM payroll_records GROUP BY Department"
    result = pd.read_sql_query(query, conn)
    conn.close()
    if not result.empty:
        logging.info("Data validation successful. Summary of net pay by department:")
        print(result)
    else:
        logging.error("Data validation failed. No records found in the database.")
    
def load_aggregated(all_dataframes, all_dept_summaries, all_warnings):
    logging.info("Loading aggregated data into SQLite database...")
    conn = sqlite3.connect(DB_PATH)
    
    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    combined_warnings = pd.concat(all_warnings, ignore_index=True)
    
    # Aggregate department summaries properly
    combined_dept_summary = pd.concat(all_dept_summaries, ignore_index=True)
    final_dept_summary = combined_dept_summary.groupby('Department').agg({
        'Gross Pay': 'sum',
        'Tax': 'sum', 
        'Net Pay': 'sum',
        'Employee Count': 'sum'
    }).reset_index()
    
    # Load to database
    combined_df.to_sql('payroll_records', conn, if_exists='replace', index=False)
    final_dept_summary.to_sql('department_summary', conn, if_exists='replace', index=False)
    combined_warnings.to_sql('overtime_warnings', conn, if_exists='replace', index=False)
    
    conn.close()
    logging.info("Aggregated data successfully loaded into SQLite database")
    return final_dept_summary


def create_export_folder():
    """Create a timestamped folder for exports in the script directory"""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_name = f"payroll_exports_{timestamp}"
    folder_path = os.path.join(SCRIPT_DIR, folder_name)
    
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logging.info(f"Created export folder: {folder_path}")
    
    return folder_path

def top_earners(export_csv=True, export_folder=None):
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT [Emp ID], [Emp Name], [Department], [Net Pay]
    FROM payroll_records
    ORDER BY [Net Pay] DESC
    LIMIT 5
    """
    
    result = pd.read_sql_query(query, conn)
    conn.close()
    
    if export_csv and not result.empty and export_folder:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        csv_filename = os.path.join(export_folder, f'top_earners_{timestamp}.csv')
        result.to_csv(csv_filename, index=False)
        logging.info(f"Top earners exported to {csv_filename}")
        print(f"Exported to: {csv_filename}")
    
    logging.info("Top 5 earners retrieved successfully.")
    print(result)
    return result

def monthly_payroll_summary(export_csv=True, export_folder=None):
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT strftime('%Y-%m', [Pay Date]) as Month, SUM([Net Pay]) as Total_Net_Pay
    FROM payroll_records
    GROUP BY strftime('%Y-%m', [Pay Date])
    ORDER BY Month
    """
    
    result = pd.read_sql_query(query, conn)
    conn.close()
    
    # Plotting data if available
    if not result.empty:
        plt.figure(figsize=(10, 6)) 
        plt.bar(result['Month'], result['Total_Net_Pay']) 
        plt.xlabel("Month")
        plt.ylabel("Total Net Pay")
        plt.title("Monthly Payroll Cost")
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save the chart
        if export_folder:
            chart_path = os.path.join(export_folder, "monthly_payroll_chart.png")
            plt.savefig(chart_path)
            print(f"Chart saved to: {chart_path}")
        else:
            chart_path = os.path.join(SCRIPT_DIR, "monthly_payroll_chart.png")
            plt.savefig(chart_path)
            print(f"Chart saved to: {chart_path}")
        
        plt.show()
        plt.close()  
    else:
        print("No data available for plotting.")
    
    # Export CSV if requested
    if export_csv and not result.empty and export_folder:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        csv_filename = os.path.join(export_folder, f'monthly_payroll_summary_{timestamp}.csv')
        result.to_csv(csv_filename, index=False)
        logging.info(f"Monthly summary exported to {csv_filename}")
        print(f"Exported to: {csv_filename}")
    
    logging.info("Total Payroll Cost Per Month")
    print(result)
    return result

def avg_hours_by_department(export_csv=True, export_folder=None):
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT Department, AVG([Hours Worked]) as Avg_Hours_Worked
    FROM payroll_records
    GROUP BY Department
    """
    
    result = pd.read_sql_query(query, conn)
    conn.close()
    
    if export_csv and not result.empty and export_folder:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        csv_filename = os.path.join(export_folder, f'avg_hours_by_department_{timestamp}.csv')
        result.to_csv(csv_filename, index=False)
        logging.info(f"Average hours by department exported to {csv_filename}")
        print(f"Exported to: {csv_filename}")
    
    logging.info("Average Hours Worked by Department")
    print(result)
    return result

def export_all_reports(export_folder):
    """Export all SQL query results to CSV files in specified folder"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        print(f"\n{'='*60}")
        print(f"EXPORTING ALL REPORTS TO CSV - {timestamp}")
        print(f"Export Folder: {export_folder}")
        print(f"{'='*60}")
        
        # Export top earners 
        print("\n1. Exporting Top Earners...")
        conn = sqlite3.connect(DB_PATH)
        top_query = """
        SELECT [Emp ID], [Emp Name], [Department], [Net Pay]
        FROM payroll_records
        ORDER BY [Net Pay] DESC
        LIMIT 5
        """
        top_df = pd.read_sql_query(top_query, conn)
        conn.close()
        
        if not top_df.empty:
            top_csv = os.path.join(export_folder, f'top_earners_{timestamp}.csv')
            top_df.to_csv(top_csv, index=False)
            print(f"Top earners exported to: {top_csv}")

        # Export monthly summary
        print("\n2. Exporting Monthly Payroll Summary...")
        conn = sqlite3.connect(DB_PATH)
        monthly_query = """
        SELECT strftime('%Y-%m', [Pay Date]) as Month, SUM([Net Pay]) as Total_Net_Pay
        FROM payroll_records
        GROUP BY strftime('%Y-%m', [Pay Date])
        ORDER BY Month
        """
        monthly_df = pd.read_sql_query(monthly_query, conn)
        conn.close()
        
        if not monthly_df.empty:
            monthly_csv = os.path.join(export_folder, f'monthly_payroll_summary_{timestamp}.csv')
            monthly_df.to_csv(monthly_csv, index=False)
            print(f"Monthly summary exported to: {monthly_csv}")

        # Export department hours
        print("\n3. Exporting Average Hours by Department...")
        conn = sqlite3.connect(DB_PATH)
        dept_hours_query = """
        SELECT Department, AVG([Hours Worked]) as Avg_Hours_Worked
        FROM payroll_records
        GROUP BY Department
        """
        dept_hours_df = pd.read_sql_query(dept_hours_query, conn)
        conn.close()
        
        if not dept_hours_df.empty:
            dept_hours_csv = os.path.join(export_folder, f'avg_hours_by_department_{timestamp}.csv')
            dept_hours_df.to_csv(dept_hours_csv, index=False)
            print(f"Average hours by department exported to: {dept_hours_csv}")
        
        # Export complete payroll records
        print("\n4. Exporting Complete Payroll Records...")
        conn = sqlite3.connect(DB_PATH)
        full_query = "SELECT * FROM payroll_records"
        full_df = pd.read_sql_query(full_query, conn)
        conn.close()
        
        if not full_df.empty:
            full_csv = os.path.join(export_folder, f'complete_payroll_records_{timestamp}.csv')
            full_df.to_csv(full_csv, index=False)
            print(f"Complete records exported to: {full_csv}")
        
        # Export department summary
        print("\n5. Exporting Department Summary...")
        conn = sqlite3.connect(DB_PATH)
        dept_query = "SELECT * FROM department_summary"
        dept_df = pd.read_sql_query(dept_query, conn)
        conn.close()
        
        if not dept_df.empty:
            dept_csv = os.path.join(export_folder, f'department_summary_{timestamp}.csv')
            dept_df.to_csv(dept_csv, index=False)
            print(f"Department summary exported to: {dept_csv}")
        
        # Export overtime warnings
        print("\n6. Exporting Overtime Warnings...")
        conn = sqlite3.connect(DB_PATH)
        overtime_query = "SELECT * FROM overtime_warnings"
        overtime_df = pd.read_sql_query(overtime_query, conn)
        conn.close()
        
        if not overtime_df.empty:
            overtime_csv = os.path.join(export_folder, f'overtime_warnings_{timestamp}.csv')
            overtime_df.to_csv(overtime_csv, index=False)
            print(f"Overtime warnings exported to: {overtime_csv}")
        
        print(f"\n{'='*60}")
        print("ALL REPORTS EXPORTED SUCCESSFULLY!")
        print(f"All files saved in folder: {export_folder}")
        print(f"{'='*60}")
        
    except sqlite3.OperationalError as e:
        logging.error(f"Database error during export: {e}")
        print("Error: Database not found or empty. Please run payroll processing first.")
    except Exception as e:
        logging.error(f"Error exporting reports: {e}")
        print(f"An error occurred during export: {e}")

def run_analysis(export_to_csv=True, export_folder=None):
    """Run all analysis functions individually - DISPLAY ONLY, NO CSV EXPORT"""
    try:
        print("\n" + "="*60)
        print("PAYROLL ANALYSIS REPORTS")
        print("="*60)
        
        print("\n--- TOP 5 EARNERS ---")
        top_earners(export_csv=False, export_folder=None)  
        
        print("\n--- MONTHLY PAYROLL SUMMARY ---")
        monthly_payroll_summary(export_csv=False, export_folder=None)  
        
        print("\n--- AVERAGE HOURS BY DEPARTMENT ---")
        avg_hours_by_department(export_csv=False, export_folder=None)  
        
        print("\n" + "="*60)
        print("ANALYSIS COMPLETE")
        print("="*60)
        
    except sqlite3.OperationalError as e:
        logging.error(f"Database error: {e}")
        print("Error: Database not found or empty. Please run payroll processing first.")
    except Exception as e:
        logging.error(f"Error running analysis: {e}")
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    print_file_locations()  # Show user where files will be saved
    
    if len(sys.argv) < 2:
        logging.error("Usage: python payroll_automation.py <data_folder>")
        print("Alternative: Run analysis on existing database")
        
        # Creating export folder for analysis only
        export_folder = create_export_folder()
        run_analysis()  
        export_all_reports(export_folder)  
    else:
        data_folder = sys.argv[1]

        if not os.path.isdir(data_folder):
            logging.error(f"Provided path is not a directory: {data_folder}")
            print("Running analysis on existing database instead...")
            
            export_folder = create_export_folder()
            run_analysis()  
            export_all_reports(export_folder)  
        else:
            logging.info(f"Using data folder: {data_folder}...")
            
            # Store results from all files
            all_dataframes = []
            all_dept_summaries = []
            all_warnings = []
            
            for file_name in os.listdir(data_folder):
                if file_name.endswith(('.xlsx', '.xls', '.csv')):
                    file_path = os.path.join(data_folder, file_name)
                    logging.info(f"Processing file: {file_path}")
                    
                    raw_data = extract(file_path)
                    if raw_data is not None:
                        result = transform(raw_data)
                        if result is not None:
                            cleaned_data, dept_summary, warnings = result
                            all_dataframes.append(cleaned_data)
                            all_dept_summaries.append(dept_summary)
                            all_warnings.append(warnings)
            
            # Load aggregated data 
            if all_dataframes:
                final_dept_summary = load_aggregated(all_dataframes, all_dept_summaries, all_warnings)
                validate_load()
                
                # Create export folder and run analysis
                export_folder = create_export_folder()
                run_analysis()  
                export_all_reports(export_folder)  
                
                logging.info("All files processed successfully.")
                print(f"\nPayroll processing complete!")
                print(f"Excel files: Check {SCRIPT_DIR}")
                print(f"CSV files: Check {export_folder}")
                print(f"Database: {DB_PATH}")
            else:
                logging.error("No valid files were processed.")
                print("Running analysis on existing database...")
                export_folder = create_export_folder()
                run_analysis() 
                export_all_reports(export_folder) 

