import pandas as pd
import logging
import unicodedata
import re
from sqlalchemy import desc, select
from models import Company, Region, Kved, CompanySize, Financial
from app import db

def clean_text_value(value):
    """Clean text values to avoid UTF-8 encoding issues"""
    if pd.isna(value) or value is None:
        return None
    
    try:
        text = str(value).strip()
        # Normalize unicode characters
        text = unicodedata.normalize('NFKC', text)
        # Remove problematic characters that cause encoding issues
        text = re.sub(r'[^\x00-\x7F\u0400-\u04FF]', '', text)  # Keep ASCII and Cyrillic
        return text if text else None
    except Exception:
        return None

def clean_numeric_value(value):
    """Clean and convert numeric values from Excel"""
    if pd.isna(value) or value == '' or value is None:
        return None
    
    if isinstance(value, str):
        # Remove spaces, commas, and other non-numeric characters except decimal points
        value = value.replace(' ', '').replace(',', '').replace('₴', '').replace('грн', '')
        # Handle different decimal separators
        if ',' in value and '.' not in value:
            value = value.replace(',', '.')
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def process_excel_file(filepath):
    """Process an Excel or CSV file with company data - safe version"""
    try:
        logging.info("🔄 Етап 1/5: Читання файлу...")
        
        # Read the file
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, encoding='utf-8')
        else:
            df = pd.read_excel(filepath)
        
        logging.info(f"📊 Завантажено {len(df)} рядків з {len(df.columns)} колонками")
        
        # Normalize column names
        logging.info("🔄 Етап 2/5: Нормалізація колонок...")
        df.columns = df.columns.str.lower().str.strip()
        
        column_mapping = {
            'код єдрпоу': 'edrpou',
            'єдрпоу': 'edrpou',
            'edrpou': 'edrpou',
            'название компании': 'name',
            'назва': 'name',
            'name': 'name',
            'квед': 'kved_code',
            'kved': 'kved_code',
            'основний вид діяльності (квед)': 'kved_description',
            'персонал (2019 р.)': 'personnel_2019',
            'область': 'region',
            'region': 'region',
            'tелефон': 'phone',
            'телефон': 'phone',
            'phone': 'phone',
            'адреса реєстрації': 'address',
            'адреса': 'address',
            'address': 'address',
            'чистий дохід від реалізації продукції': 'revenue',
            'дохід': 'revenue',
            'revenue': 'revenue',
            'чистий фінансовий результат: прибуток': 'profit',
            'прибуток': 'profit',
            'profit': 'profit',
            'размер': 'company_size',
            'розмір': 'company_size'
        }
        
        df = df.rename(columns=column_mapping)
        
        if 'edrpou' not in df.columns:
            raise ValueError("Файл повинен містити колонку ЄДРПОУ")
        
        logging.info("🔄 Етап 3/5: Створення записів компаній...")
        
        success_count = 0
        error_count = 0
        total_rows = len(df)
        
        for index, row in df.iterrows():
            try:
                edrpou = clean_text_value(row['edrpou'])
                if not edrpou or edrpou == 'nan':
                    error_count += 1
                    continue
                
                # Get personnel data
                personnel = None
                if 'personnel_2019' in row:
                    personnel = clean_numeric_value(row['personnel_2019'])
                    if personnel is not None:
                        personnel = int(personnel)
                
                # Try to create/update company with minimal database interaction
                try:
                    # Use a new session to avoid encoding issues
                    company = Company()
                    company.edrpou = edrpou
                    company.name = clean_text_value(row['name'])
                    company.phone = clean_text_value(row['phone']) if 'phone' in row and pd.notna(row['phone']) else None
                    company.address = clean_text_value(row['address']) if 'address' in row and pd.notna(row['address']) else None
                    company.personnel_2019 = personnel
                    # Store related data directly
                    company.region_name = clean_text_value(row.get('region'))
                    company.kved_code = clean_text_value(row.get('kved_code'))
                    company.kved_description = clean_text_value(row.get('kved_description'))
                    company.company_size_name = clean_text_value(row.get('company_size'))
                    
                    # Store financial data directly in company if present
                    revenue = clean_numeric_value(row.get('revenue'))
                    profit = clean_numeric_value(row.get('profit'))
                    if revenue is not None or profit is not None:
                        financial = Financial()
                        financial.company_edrpou = edrpou
                        financial.year = 2019
                        financial.revenue = revenue
                        financial.profit = profit
                        db.session.add(financial)
                    
                    # Save with direct SQL to avoid ORM encoding issues
                    db.session.add(company)
                    db.session.commit()
                    
                    logging.info(f"✅ Збережено компанію {company.name}")
                    success_count += 1
                    
                except Exception as save_error:
                    logging.warning(f"⚠️ Не вдалося зберегти {edrpou}: {save_error}")
                    db.session.rollback()
                    error_count += 1
                    continue
                
            except Exception as row_error:
                logging.error(f"❌ Помилка обробки рядка {index}: {row_error}")
                error_count += 1
                continue
        
        logging.info("🔄 Етап 5/5: Завершення...")
        logging.info(f"✅ Файл успішно оброблено! {success_count} компаній, {error_count} помилок.")
        return success_count, error_count
        
    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        return 0, 1

def merge_company_data(merge_filepath):
    """Merge company data from a second file - safe version"""
    try:
        # Read the merge file
        if merge_filepath.endswith('.csv'):
            df = pd.read_csv(merge_filepath, encoding='utf-8')
        else:
            df = pd.read_excel(merge_filepath)
        
        # Normalize column names
        df.columns = df.columns.str.lower().str.strip()
        column_mapping = {
            'код єдрпоу': 'edrpou',
            'єдрпоу': 'edrpou',
            'edrpou': 'edrpou',
            'телефон': 'phone',
            'phone': 'phone',
            'адреса реєстрації': 'address',
            'адреса': 'address',
            'address': 'address'
        }
        df = df.rename(columns=column_mapping)
        
        if 'edrpou' not in df.columns:
            raise ValueError("Файл злиття повинен містити колонку ЄДРПОУ")
        
        merge_count = 0
        
        for index, row in df.iterrows():
            try:
                edrpou = clean_text_value(row['edrpou'])
                if not edrpou or edrpou == 'nan':
                    continue
                
                # Try to find and update existing company
                try:
                    company = db.session.execute(select(Company).where(Company.edrpou == edrpou)).scalar_one_or_none()
                    if company:
                        updated = False
                        
                        # Update phone if provided and different
                        if 'phone' in row and pd.notna(row['phone']):
                            new_phone = clean_text_value(row['phone'])
                            if new_phone and new_phone != company.phone:
                                company.phone = new_phone
                                updated = True
                        
                        # Update address if provided and different
                        if 'address' in row and pd.notna(row['address']):
                            new_address = clean_text_value(row['address'])
                            if new_address and new_address != company.address:
                                company.address = new_address
                                updated = True
                        
                        if updated:
                            db.session.commit()
                            merge_count += 1
                            logging.info(f"📞 Оновлено контакти для {company.name}")
                
                except Exception as update_error:
                    logging.warning(f"⚠️ Помилка оновлення {edrpou}: {update_error}")
                    db.session.rollback()
                    continue
                    
            except Exception as merge_error:
                logging.error(f"❌ Помилка злиття рядка {index}: {merge_error}")
                continue
        
        logging.info(f"📞 Оновлено контактні дані для {merge_count} компаній")
        return merge_count, 0
        
    except Exception as e:
        logging.error(f"Error merging data: {str(e)}")
        return 0, 1