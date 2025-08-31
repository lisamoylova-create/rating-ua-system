import pandas as pd
import logging
from app import db
from models_simple import Company

def clean_text_value(value):
    """Clean text value for database storage"""
    if pd.isna(value) or value is None:
        return None
    
    text = str(value).strip()
    
    # Replace problematic characters
    replacements = {
        '\x00': '',  # NULL bytes
        '\r\n': ' ',
        '\n': ' ',
        '\r': ' ',
        '\t': ' ',
        '"': "'",
        '\\': '/',
        '\x01': '',
        '\x02': '',
        '\x03': '',
        '\x04': '',
        '\x05': '',
        '\x06': '',
        '\x07': '',
        '\x08': '',
        '\x0b': '',
        '\x0c': '',
        '\x0e': '',
        '\x0f': ''
    }
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    # Limit length to prevent issues
    if len(text) > 500:
        text = text[:500]
    
    return text if text else None

def clean_numeric_value(value):
    """Clean numeric value for database storage"""
    if pd.isna(value) or value is None or value == '':
        return None
    
    try:
        # Convert to string first to handle different types
        str_value = str(value).strip()
        
        # Remove common non-numeric characters
        str_value = str_value.replace(',', '').replace(' ', '').replace('₴', '').replace('грн', '')
        
        # Try to convert to float
        numeric_value = float(str_value)
        
        # Check for reasonable bounds
        if numeric_value < 0 or numeric_value > 1000000000000:  # 1 trillion limit
            return None
            
        return numeric_value
    except (ValueError, TypeError):
        return None

def process_companies_minimal(df):
    """Process companies with minimal error handling"""
    success_count = 0
    error_count = 0
    
    logging.info(f"Starting minimal processing of {len(df)} rows")
    
    for index, row in df.iterrows():
        try:
            # Get basic fields
            edrpou = clean_text_value(row.get('edrpou'))
            name = clean_text_value(row.get('name'))
            
            if not edrpou or not name:
                error_count += 1
                continue
            
            # Check if company exists
            existing = db.session.execute(
                db.select(Company).where(Company.edrpou == edrpou)
            ).scalar_one_or_none()
            
            if existing:
                # Update existing
                company = existing
                company.actualized = 'так'
            else:
                # Create new
                company = Company()
                company.edrpou = edrpou
                company.actualized = 'ні'
            
            # Set basic fields
            company.name = name
            company.phone = clean_text_value(row.get('phone'))
            company.address = clean_text_value(row.get('address'))
            
            # Personnel
            personnel = clean_numeric_value(row.get('personnel_2019'))
            company.personnel_2019 = int(personnel) if personnel else None
            
            # Related data
            company.region_name = clean_text_value(row.get('region'))
            company.kved_code = clean_text_value(row.get('kved_code'))
            company.kved_description = clean_text_value(row.get('kved_description'))
            company.company_size_name = clean_text_value(row.get('company_size'))
            
            # Financial data
            company.revenue_2019 = clean_numeric_value(row.get('revenue'))
            company.profit_2019 = clean_numeric_value(row.get('profit'))
            
            # Save immediately to avoid batch issues
            if not existing:
                db.session.add(company)
            
            db.session.commit()
            
            success_count += 1
            
            if success_count % 10 == 0:
                logging.info(f"Processed {success_count} companies")
                
        except Exception as e:
            db.session.rollback()
            logging.error(f"Error processing row {index}: {str(e)}")
            error_count += 1
            continue
    
    logging.info(f"Processing complete: {success_count} success, {error_count} errors")
    return success_count, error_count