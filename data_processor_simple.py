import pandas as pd
import logging
from app import db
from models import Company

def process_excel_file_simple(filepath):
    """Ultra-simple file processing without any database operations"""
    try:
        logging.info("🔄 Простий тест читання файлу...")
        
        # Read file
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, encoding='utf-8')
        else:
            df = pd.read_excel(filepath)
        
        logging.info(f"📊 Файл містить {len(df)} рядків та {len(df.columns)} колонок")
        logging.info(f"📋 Колонки: {list(df.columns)}")
        
        # Count companies without database operations
        if 'Код ЄДРПОУ' in df.columns or 'єдрпоу' in df.columns or 'edrpou' in df.columns:
            edrpou_col = None
            for col in ['Код ЄДРПОУ', 'єдрпоу', 'edrpou']:
                if col in df.columns:
                    edrpou_col = col
                    break
            
            if edrpou_col:
                valid_companies = df[df[edrpou_col].notna()]
                logging.info(f"✅ Знайдено {len(valid_companies)} компаній з ЄДРПОУ")
                return len(valid_companies), 0
        
        logging.info("⚠️ ЄДРПОУ колонка не знайдена")
        return 0, len(df)
        
    except Exception as e:
        logging.error(f"❌ Помилка читання файлу: {e}")
        return 0, 1

def merge_company_data_simple(merge_filepath):
    """Simple file merge test without database"""
    try:
        if merge_filepath.endswith('.csv'):
            df = pd.read_csv(merge_filepath, encoding='utf-8')
        else:
            df = pd.read_excel(merge_filepath)
        
        logging.info(f"📊 Файл для злиття містить {len(df)} рядків")
        
        # Simple count of valid EDRPOUs
        edrpou_col = None
        for col in ['Код ЄДРПОУ', 'єдрпоу', 'edrpou']:
            if col in df.columns:
                edrpou_col = col
                break
        
        if edrpou_col:
            valid_merge = df[df[edrpou_col].notna()]
            return len(valid_merge), 0
        
        return 0, len(df)
        
    except Exception as e:
        logging.error(f"❌ Помилка читання файлу злиття: {e}")
        return 0, 1