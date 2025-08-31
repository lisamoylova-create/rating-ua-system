from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, current_user
from werkzeug.security import check_password_hash, generate_password_hash
from models_full import User
from app import db

auth = Blueprint('auth', __name__)

@auth.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        remember = bool(request.form.get('remember'))
        
        if not username or not password:
            flash('Будь ласка, заповніть всі поля.', 'danger')
            return render_template('login.html')
        
        user = db.session.execute(db.select(User).where(User.username == username)).scalar_one_or_none()
        
        if user and check_password_hash(user.password_hash, password):
            login_user(user, remember=remember)
            next_page = request.args.get('next')
            if next_page:
                return redirect(next_page)
            return redirect(url_for('main.index'))
        else:
            flash('Невірний логін або пароль.', 'danger')
    
    return render_template('login.html')

@auth.route('/logout')
def logout():
    logout_user()
    flash('Ви успішно вийшли з системи.', 'info')
    return redirect(url_for('auth.login'))

@auth.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        password_confirm = request.form.get('password_confirm', '')
        
        # Validation
        errors = []
        
        if not username:
            errors.append('Ім\'я користувача обов\'язкове.')
        elif len(username) < 3:
            errors.append('Ім\'я користувача має бути не менше 3 символів.')
        elif db.session.execute(db.select(User).where(User.username == username)).scalar_one_or_none():
            errors.append('Користувач з таким іменем вже існує.')
            
        if not email:
            errors.append('Email обов\'язковий.')
        elif '@' not in email or '.' not in email:
            errors.append('Введіть коректний email.')
        elif db.session.execute(db.select(User).where(User.email == email)).scalar_one_or_none():
            errors.append('Користувач з таким email вже існує.')
            
        if not password:
            errors.append('Пароль обов\'язковий.')
        elif len(password) < 6:
            errors.append('Пароль має бути не менше 6 символів.')
            
        if password != password_confirm:
            errors.append('Паролі не співпадають.')
        
        if errors:
            for error in errors:
                flash(error, 'danger')
            return render_template('register.html')
        
        try:
            # Create new user with 'viewer' role by default
            user = User()
            user.username = username
            user.email = email
            user.password_hash = generate_password_hash(password)
            user.role = 'guest'  # Default role
            
            db.session.add(user)
            db.session.commit()
            
            flash('Реєстрацію завершено! Тепер ви можете увійти в систему.', 'success')
            return redirect(url_for('auth.login'))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Помилка реєстрації: {str(e)}', 'danger')
            return render_template('register.html')
    
    return render_template('register.html')
