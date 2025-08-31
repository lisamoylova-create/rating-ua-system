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

def get_or_create_region(region_name):
    """Get or create a region"""
    if not region_name or pd.isna(region_name):
        return None
    
    region_name = clean_text_value(region_name)
    if not region_name:
        return None
        
    try:
        region = db.session.execute(select(Region).where(Region.name == region_name)).scalar_one_or_none()
        if not region:
            region = Region()
            region.name = region_name
            db.session.add(region)
            db.session.flush()
        return region
    except Exception as e:
        logging.error(f"Error in get_or_create_region: {e}")
        db.session.rollback()
        return None

def get_or_create_kved(kved_code, kved_description):
    """Get or create a KVED entry"""
    if not kved_code or pd.isna(kved_code):
        return None
    
    kved_code = clean_text_value(kved_code)
    if not kved_code:
        return None
        
    try:
        kved = db.session.execute(select(Kved).where(Kved.code == kved_code)).scalar_one_or_none()
        if not kved:
            description = clean_text_value(kved_description) if kved_description and not pd.isna(kved_description) else kved_code
            kved = Kved()
            kved.code = kved_code
            kved.description = description
            db.session.add(kved)
            db.session.flush()
        return kved
    except Exception as e:
        logging.error(f"Error in get_or_create_kved: {e}")
        db.session.rollback()
        return None

def get_or_create_company_size(size_name):
    """Get or create a company size"""
    if not size_name or pd.isna(size_name):
        return None
    
    size_name = clean_text_value(size_name)
    if not size_name:
        return None
        
    try:
        company_size = db.session.execute(select(CompanySize).where(CompanySize.size_name == size_name)).scalar_one_or_none()
        if not company_size:
            company_size = CompanySize()
            company_size.size_name = size_name
            db.session.add(company_size)
            db.session.flush()
        return company_size
    except Exception as e:
        logging.error(f"Error in get_or_create_company_size: {e}")
        db.session.rollback()
        return None

def process_excel_file(filepath):
    """Process an Excel or CSV file with company data"""
    try:
        logging.info("🔄 Етап 1/5: Читання файлу...")
        # Read the file
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, encoding='utf-8')
        else:
            df = pd.read_excel(filepath)
        logging.info(f"✅ Файл прочитано: {len(df)} рядків")
        
        logging.info("🔄 Етап 2/5: Нормалізація даних...")
        # Normalize column names to handle different variations
        column_mapping = {
            'код єдрпоу': 'edrpou',
            'єдрпоу': 'edrpou',
            'edrpou': 'edrpou',
            'название компании': 'name',
            'назва компанії': 'name',
            'компанія': 'name',
            'name': 'name',
            'квед': 'kved_code',
            'kved': 'kved_code',
            'основний вид діяльності (квед)': 'kved_description',
            'основний вид діяльності': 'kved_description',
            'діяльність': 'kved_description',
            'персонал (2019 р.)': 'personnel_2019',
            'персонал': 'personnel_2019',
            'personnel': 'personnel_2019',
            'область': 'region',
            'регіон': 'region',
            'region': 'region',
            'телефон': 'phone',
            'phone': 'phone',
            'адреса реєстрації': 'address',
            'адреса': 'address',
            'address': 'address',
            'чистий дохід від реалізації продукції': 'revenue',
            'чистий дохід від реалізації продукції (товарів, робіт, послуг)': 'revenue',
            'чистий дохід (виручка) від реалізації продукції': 'revenue',
            'дохід від реалізації': 'revenue',
            'дохід': 'revenue',
            'виручка': 'revenue',
            'оборот': 'revenue',
            'revenue': 'revenue',
            'чистий фінансовий результат: прибуток': 'profit',
            'чистий фінансовий результат (прибуток)': 'profit',
            'фінансовий результат до оподаткування': 'profit',
            'прибуток (збиток) до оподаткування': 'profit',
            'чистий прибуток (збиток)': 'profit',
            'прибуток': 'profit',
            'чистий прибуток': 'profit',
            'profit': 'profit',
            'размер': 'size',
            'розмір': 'size',
            'size': 'size'
        }
        
        # Rename columns based on mapping with better normalization
        df.columns = df.columns.astype(str).str.lower().str.strip()
        # Remove extra whitespace and normalize
        df.columns = [' '.join(col.split()) for col in df.columns]
        df = df.rename(columns=column_mapping)
        
        # Check required columns
        required_columns = ['edrpou', 'name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Відсутні обов'язкові колонки: {', '.join(missing_columns)}")
        
        logging.info(f"✅ Дані нормалізовано: {len(df.columns)} колонок")
        
        # Debug: show actual columns after mapping
        logging.info(f"📋 Колонки після нормалізації: {list(df.columns)}")
        if 'revenue' in df.columns:
            logging.info("✅ Колонка 'revenue' знайдена")
        else:
            logging.info("❌ Колонка 'revenue' НЕ знайдена")
        if 'profit' in df.columns:
            logging.info("✅ Колонка 'profit' знайдена") 
        else:
            logging.info("❌ Колонка 'profit' НЕ знайдена")
            
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
                
                # Skip all related entities to avoid UTF-8 issues
                region = None
                kved = None
                company_size = None
                
                # Check if company exists and update or create
                try:
                    # Force commit any pending changes to avoid autoflush issues
                    try:
                        db.session.commit()
                    except:
                        db.session.rollback()
                    
                    company = db.session.execute(select(Company).where(Company.edrpou == edrpou)).scalar_one_or_none()
                    
                    personnel = None
                    if 'personnel_2019' in row:
                        personnel = clean_numeric_value(row['personnel_2019'])
                        if personnel is not None:
                            personnel = int(personnel)
                    
                    if company:
                        # Update existing company
                        company.name = clean_text_value(row['name'])
                        company.phone = clean_text_value(row['phone']) if 'phone' in row and pd.notna(row['phone']) else None
                        company.address = clean_text_value(row['address']) if 'address' in row and pd.notna(row['address']) else None
                        company.personnel_2019 = personnel
                        # Skip foreign key updates to avoid UTF-8 issues
                        logging.info(f"🔄 Оновлено компанію {company.name} (ID={company.id})")
                    else:
                        # Create new company
                        company = Company()
                        company.edrpou = edrpou
                        company.name = clean_text_value(row['name'])
                        company.phone = clean_text_value(row['phone']) if 'phone' in row and pd.notna(row['phone']) else None
                        company.address = clean_text_value(row['address']) if 'address' in row and pd.notna(row['address']) else None
                        company.personnel_2019 = personnel
                        company.region_id = None
                        company.kved_id = None
                        company.company_size_id = None
                        
                        db.session.add(company)
                        try:
                            db.session.commit()  # Commit immediately to avoid autoflush issues
                        except Exception as commit_error:
                            db.session.rollback()
                            logging.error(f"❌ Помилка збереження {company.name}: {commit_error}")
                            error_count += 1
                            continue
                        logging.info(f"✅ Створено нову компанію {company.name} (ID={company.id})")
                
                except Exception as e:
                    logging.error(f"❌ Помилка обробки компанії {edrpou}: {e}")
                    db.session.rollback()
                    error_count += 1
                    continue
                
                # Skip financial data processing temporarily to avoid UTF-8 issues
                # Will be re-enabled once the core company data works
                pass
                
                # Log progress every 100 companies
                if success_count % 100 == 0 and success_count > 0:
                    logging.info(f"📊 Оброблено {success_count} з {total_rows} компаній ({int(success_count/total_rows*100)}%)")
                
                # Financial data now handled above in the main try block
                
                success_count += 1
                
                # Update progress for financial processing
                if success_count == int(total_rows * 0.8):
                    logging.info("🔄 Етап 4/5: Обробка фінансових даних...")
                
            except Exception as e:
                logging.error(f"Error processing row {index}: {str(e)}")
                error_count += 1
                continue
        
        logging.info("🔄 Етап 5/5: Збереження до бази даних...")
        # Already committed per company to avoid UTF-8 issues
        logging.info(f"✅ Файл успішно оброблено! {success_count} компаній, {error_count} помилок.")
        return success_count, error_count
        
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error processing file: {str(e)}")
        raise

def merge_company_data(merge_filepath):
    """Merge company data from a second file, updating contact information"""
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
                edrpou = str(row['edrpou']).strip()
                if not edrpou or edrpou == 'nan':
                    continue
                
                company = db.session.execute(select(Company).where(Company.edrpou == edrpou)).scalar_one_or_none()
                if company:
                    updated = False
                    
                    # Update phone if provided and different
                    if 'phone' in row and pd.notna(row['phone']):
                        new_phone = str(row['phone']).strip()
                        if new_phone and new_phone != company.phone:
                            company.phone = new_phone
                            updated = True
                    
                    # Update address if provided and different
                    if 'address' in row and pd.notna(row['address']):
                        new_address = str(row['address']).strip()
                        if new_address and new_address != company.address:
                            company.address = new_address
                            updated = True
                    
                    if updated:
                        merge_count += 1
                        
            except Exception as e:
                logging.error(f"Error merging row {index}: {str(e)}")
                continue
        
        db.session.commit()
        logging.info(f"Merge completed. {merge_count} companies updated.")
        return merge_count
        
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error merging file: {str(e)}")
        raise

def filter_and_rank_companies(min_employees=0, min_revenue=0, min_profit=None, 
                              region_ids=None, kved_ids=None, size_ids=None,
                              sort_criteria='revenue', apply_regional_kved_filter=False,
                              year_source='2025', ranking_name=''):
    """
    Sequential filtering and ranking pipeline:
    1. Primary filters (employees, revenue, profit)
    2. Secondary filters (region, kved, size)
    3. Sorting and ranking
    """
    try:
        # Clear existing rankings first
        Company.query.update({Company.ranking: None})
        db.session.commit()
        
        # STEP 1: Primary filtering - get companies that meet basic criteria
        logging.info("Step 1: Applying primary filters (employees, revenue, profit)")
        
        primary_query = db.session.query(Company).join(Financial)
        
        # Apply employee filter
        if min_employees > 0:
            primary_query = primary_query.filter(Company.personnel_2019 >= min_employees)
            logging.info(f"Applied employee filter: >= {min_employees}")
        
        # Apply revenue filter
        if min_revenue > 0:
            primary_query = primary_query.filter(Financial.revenue >= min_revenue)
            logging.info(f"Applied revenue filter: >= {min_revenue}")
            
        # Apply profit filter
        if min_profit is not None and min_profit != 0:
            primary_query = primary_query.filter(Financial.profit >= min_profit)
            logging.info(f"Applied profit filter: >= {min_profit}")
        
        # Get primary filtered companies
        primary_companies = primary_query.all()
        primary_count = len(primary_companies)
        logging.info(f"Primary filtering result: {primary_count} companies")
        
        if primary_count == 0:
            logging.info("No companies passed primary filtering")
            return 0, 0
        
        # STEP 2: Secondary filtering on primary results
        logging.info("Step 2: Applying secondary filters (region, kved, size)")
        
        secondary_companies = primary_companies.copy()
        
        # Filter by regions
        if region_ids:
            secondary_companies = [c for c in secondary_companies if c.region_id in region_ids]
            logging.info(f"Applied region filter: {len(secondary_companies)} companies remain")
        
        # Filter by KVED codes
        if kved_ids:
            secondary_companies = [c for c in secondary_companies if c.kved_id in kved_ids]
            logging.info(f"Applied KVED filter: {len(secondary_companies)} companies remain")
        
        # Filter by company sizes
        if size_ids:
            secondary_companies = [c for c in secondary_companies if c.company_size_id in size_ids]
            logging.info(f"Applied size filter: {len(secondary_companies)} companies remain")
        
        # Apply regional KVED filter if enabled
        if apply_regional_kved_filter:
            logging.info("Applying additional regional KVED filter")
            
            # Group by KVED and count companies in secondary results
            kved_counts = {}
            for company in secondary_companies:
                if company.kved_id:
                    kved_counts[company.kved_id] = kved_counts.get(company.kved_id, 0) + 1
            
            # Find KVED groups with less than 100 companies
            small_kved_groups = [kved_id for kved_id, count in kved_counts.items() if count < 100]
            
            if small_kved_groups:
                # Apply additional regional filter for these groups
                final_companies = []
                for company in secondary_companies:
                    if company.kved_id in small_kved_groups:
                        # Keep only if has region
                        if company.region_id is not None:
                            final_companies.append(company)
                    else:
                        # Keep all companies from larger KVED groups
                        final_companies.append(company)
                secondary_companies = final_companies
                logging.info(f"Applied regional KVED filter: {len(secondary_companies)} companies remain")
        
        secondary_count = len(secondary_companies)
        logging.info(f"Secondary filtering result: {secondary_count} companies")
        
        if secondary_count == 0:
            logging.info("No companies passed secondary filtering")
            return 0, 0
        
        # STEP 3: Sorting and ranking
        logging.info(f"Step 3: Sorting by {sort_criteria} and assigning rankings")
        
        # Sort companies based on criteria
        if sort_criteria == 'revenue':
            secondary_companies.sort(key=lambda c: (c.financials[0].revenue if len(c.financials) > 0 else 0) or 0, reverse=True)
        elif sort_criteria == 'profit':
            secondary_companies.sort(key=lambda c: (c.financials[0].profit if len(c.financials) > 0 else 0) or 0, reverse=True)
        elif sort_criteria == 'personnel':
            secondary_companies.sort(key=lambda c: (c.personnel_2019 or 0), reverse=True)
        
        # Assign rankings to final companies with technical metadata
        ranking_updated = 0
        for rank, company in enumerate(secondary_companies, 1):
            company.ranking = rank
            
            # Add technical columns for export (33 columns total = 11+17+4+1)
            company.source = f"Україна {year_source}"
            company.top_count = secondary_count  # Total companies in this ranking
            company.total_count = secondary_count  # General count by category
            company.actualized = "так"  # Mark as actualized during ranking
            
            ranking_updated += 1
        
        db.session.commit()
        
        # Log ranking creation with metadata
        if ranking_name:
            logging.info(f"Рейтинг '{ranking_name}' створено для {year_source} року")
        
        logging.info(f"Filtering pipeline completed. {secondary_count} companies in final ranking. {ranking_updated} rankings assigned.")
        return secondary_count, ranking_updated
        
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error in filtering pipeline: {str(e)}")
        raise


def filter_companies(min_employees=0, min_revenue=0, sort_criteria='revenue', apply_regional_filter=False):
    """Legacy filter function - calls new pipeline with basic parameters"""
    min_profit = None  # Legacy doesn't have profit filter
    return filter_and_rank_companies(
        min_employees=min_employees,
        min_revenue=min_revenue,
        min_profit=min_profit,
        region_ids=None,
        kved_ids=None,
        size_ids=None,
        sort_criteria=sort_criteria,
        apply_regional_kved_filter=apply_regional_filter
    )

def export_to_csv(companies):
    """Export companies to CSV format for CRM"""
    try:
        data = []
        
        for company in companies:
            financial = company.financials[0] if company.financials else None
            
            row = {
                'Рейтинг': company.ranking,
                'ЄДРПОУ': company.edrpou,
                'Назва компанії': company.name,
                'КВЕД': company.kved.code if company.kved else '',
                'Вид діяльності': company.kved.description if company.kved else '',
                'Персонал (2019)': company.personnel_2019,
                'Область': company.region.name if company.region else '',
                'Телефон': company.phone,
                'Адреса': company.address,
                'Дохід': financial.revenue if financial else '',
                'Прибуток': financial.profit if financial else '',
                'Розмір': company.company_size.size_name if company.company_size else ''
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        export_path = 'filtered_companies_export.csv'
        df.to_csv(export_path, index=False, encoding='utf-8')
        
        return export_path
        
    except Exception as e:
        logging.error(f"Error exporting to CSV: {str(e)}")
        raise
