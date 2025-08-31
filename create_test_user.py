#!/usr/bin/env python3

from app import app, db
from models import User
from werkzeug.security import generate_password_hash

def create_test_user():
    with app.app_context():
        # Check if test user already exists
        existing_user = db.session.query(User).filter_by(username='admin').first()
        
        if existing_user:
            print("Тестовий користувач 'admin' вже існує")
            return
        
        # Create test user
        user = User()
        user.username = 'admin'
        user.email = 'admin@example.com'
        user.password_hash = generate_password_hash('admin123')
        user.role = 'admin'
        
        db.session.add(user)
        db.session.commit()
        
        print("✅ Створено тестового користувача:")
        print("   Логін: admin")
        print("   Пароль: admin123")
        print("   Роль: admin")

if __name__ == '__main__':
    create_test_user()