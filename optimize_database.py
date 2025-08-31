#!/usr/bin/env python3
"""
Оптимізація бази даних для швидшого завантаження
"""

from app import app, db
import logging

def optimize_database():
    """Create indexes and optimize database for faster imports"""
    with app.app_context():
        try:
            # Створюємо індекс на ЄДРПОУ для швидшого пошуку
            db.session.execute(db.text("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_edrpou 
                ON companies(edrpou);
            """))
            
            # Індекс на назву компанії для швидшого пошуку
            db.session.execute(db.text("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_name 
                ON companies(name);
            """))
            
            # Індекс на КВЕД код
            db.session.execute(db.text("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_kved 
                ON companies(kved_code);
            """))
            
            # Індекс на регіон
            db.session.execute(db.text("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_region 
                ON companies(region_name);
            """))
            
            # Оптимізуємо налаштування PostgreSQL (тільки дозволені в production)
            try:
                db.session.execute(db.text("SET synchronous_commit = off;"))
                print("✓ Synchronous commit відключено")
            except Exception as e:
                print(f"! Не вдалося змінити synchronous_commit: {e}")
            
            # Інші параметри недоступні в managed PostgreSQL
            print("! Деякі оптимізації недоступні в production режимі")
            
            db.session.commit()
            print("✓ Індекси створено успішно")
            print("✓ Налаштування оптимізації застосовано")
            
        except Exception as e:
            print(f"Помилка оптимізації: {e}")
            db.session.rollback()

def analyze_tables():
    """Аналіз таблиць для оптимізації планувальника запитів"""
    with app.app_context():
        try:
            db.session.execute(db.text("ANALYZE companies;"))
            db.session.commit()
            print("✓ Аналіз таблиці companies завершено")
        except Exception as e:
            print(f"Помилка аналізу: {e}")

if __name__ == '__main__':
    print("Початок оптимізації бази даних...")
    optimize_database()
    analyze_tables()
    print("Оптимізація завершена!")