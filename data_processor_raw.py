import pandas as pd
import logging
import psycopg2
import os
from decimal import Decimal

def get_db_connection():
    """Get direct PostgreSQL connection"""
    return psycopg2.connect(os.environ.get("DATABASE_URL"))

def clean_text_for_sql(value):
    """Clean text value for SQL insertion"""
    if pd.isna(value) or value is None:
        return None
    
    text = str(value).strip()
    
    # Remove NULL bytes and other problematic characters
    text = text.replace('\x00', '').replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    text = text.replace('\t', ' ').replace('\\', '/').replace("'", "''")  # Escape single quotes
    
    # Limit length
    if len(text) > 500:
        text = text[:500]
    
    return text if text else None

def clean_numeric_for_sql(value):
    """Clean numeric value for SQL insertion"""
    if pd.isna(value) or value is None or value == '':
        return None
    
    try:
        str_value = str(value).strip()
        str_value = str_value.replace(',', '').replace(' ', '').replace('₴', '').replace('грн', '')
        
        numeric_value = float(str_value)
        
        if numeric_value < 0 or numeric_value > 1000000000000:
            return None
            
        return numeric_value
    except (ValueError, TypeError):
        return None

def process_companies_raw_sql(df):
    """Process companies using raw SQL to avoid encoding issues"""
    success_count = 0
    error_count = 0
    
    logging.info(f"Starting raw SQL processing of {len(df)} rows")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for index, row in df.iterrows():
            try:
                # Get basic fields
                edrpou = clean_text_for_sql(row.get('edrpou'))
                name = clean_text_for_sql(row.get('name'))
                
                if not edrpou or not name:
                    error_count += 1
                    continue
                
                # Check if company exists
                cursor.execute("SELECT id FROM companies WHERE edrpou = %s", (edrpou,))
                existing = cursor.fetchone()
                
                # Prepare data
                phone = clean_text_for_sql(row.get('phone'))
                address = clean_text_for_sql(row.get('address'))
                personnel = clean_numeric_for_sql(row.get('personnel_2019'))
                personnel = int(personnel) if personnel else None
                
                region_name = clean_text_for_sql(row.get('region'))
                kved_code = clean_text_for_sql(row.get('kved_code'))
                kved_description = clean_text_for_sql(row.get('kved_description'))
                company_size_name = clean_text_for_sql(row.get('company_size'))
                
                revenue = clean_numeric_for_sql(row.get('revenue'))
                profit = clean_numeric_for_sql(row.get('profit'))
                
                if existing:
                    # Update existing company
                    company_id = existing[0]
                    cursor.execute("""
                        UPDATE companies SET 
                            name = %s, phone = %s, address = %s, personnel_2019 = %s,
                            region_name = %s, kved_code = %s, kved_description = %s, 
                            company_size_name = %s, revenue_2019 = %s, profit_2019 = %s,
                            actualized = 'так', updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (name, phone, address, personnel, region_name, kved_code, 
                          kved_description, company_size_name, revenue, profit, company_id))
                else:
                    # Insert new company
                    cursor.execute("""
                        INSERT INTO companies 
                        (edrpou, name, phone, address, personnel_2019, region_name, 
                         kved_code, kved_description, company_size_name, revenue_2019, 
                         profit_2019, source, actualized, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, (edrpou, name, phone, address, personnel, region_name, 
                          kved_code, kved_description, company_size_name, revenue, 
                          profit, 'основний', 'ні'))
                
                conn.commit()
                success_count += 1
                
                if success_count % 10 == 0:
                    logging.info(f"Processed {success_count} companies")
                    
            except Exception as e:
                conn.rollback()
                logging.error(f"Error processing row {index}: {str(e)}")
                error_count += 1
                continue
    
    finally:
        cursor.close()
        conn.close()
    
    logging.info(f"Raw SQL processing complete: {success_count} success, {error_count} errors")
    return success_count, error_count