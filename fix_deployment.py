#!/usr/bin/env python3
"""
Виправлення deployment проблем з базою даних
"""

from app import app, db
import logging

def fix_deployment_issues():
    """Fix database issues for deployment"""
    with app.app_context():
        try:
            # Перевірка стану бази даних
            print("Перевіряємо стан бази даних...")
            
            # Перевіряємо чи існують основні таблиці
            result = db.session.execute(db.text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public';
            """))
            
            tables = [row[0] for row in result.fetchall()]
            print(f"Існуючі таблиці: {tables}")
            
            if 'companies' not in tables:
                print("Створюємо таблицю companies...")
                db.create_all()
                print("✓ Таблиці створено")
            else:
                print("✓ Таблиця companies вже існує")
            
            # Перевіряємо кількість записів
            count_result = db.session.execute(db.text("SELECT COUNT(*) FROM companies;"))
            count = count_result.scalar()
            print(f"✓ Кількість компаній в базі: {count}")
            
            # Очищаємо тимчасові дані для deployment
            print("Очищаємо тимчасові дані...")
            db.session.execute(db.text("DROP TABLE IF EXISTS temp_companies CASCADE;"))
            db.session.commit()
            print("✓ Тимчасові таблиці очищено")
            
            print("✓ База даних готова до deployment")
            
        except Exception as e:
            print(f"Помилка: {e}")
            db.session.rollback()

if __name__ == '__main__':
    fix_deployment_issues()