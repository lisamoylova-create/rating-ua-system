from functools import wraps
from flask import flash, redirect, url_for, request, abort
from flask_login import current_user

def require_permission(permission):
    """Декоратор для перевірки дозволів користувача"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                flash('Будь ласка, увійдіть для доступу до цієї сторінки.', 'warning')
                return redirect(url_for('auth.login', next=request.url))
            
            if not current_user.has_permission(permission):
                flash('У вас немає дозволу для цієї дії.', 'danger')
                return abort(403)
                
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def require_role(role):
    """Декоратор для перевірки конкретної ролі"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                flash('Будь ласка, увійдіть для доступу до цієї сторінки.', 'warning')
                return redirect(url_for('auth.login', next=request.url))
            
            if current_user.role != role:
                flash('У вас немає дозволу для цієї дії.', 'danger')
                return abort(403)
                
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# Конкретні декоратори для різних ролей
def admin_required(f):
    return require_role('admin')(f)

def manager_or_admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Будь ласка, увійдіть для доступу до цієї сторінки.', 'warning')
            return redirect(url_for('auth.login', next=request.url))
        
        if current_user.role not in ['admin', 'manager']:
            flash('У вас немає дозволу для цієї дії.', 'danger')
            return abort(403)
            
        return f(*args, **kwargs)
    return decorated_function

# Декоратори для конкретних дозволів
def upload_required(f):
    return require_permission('upload')(f)

def actualize_required(f):
    return require_permission('actualize')(f)

def export_required(f):
    return require_permission('export')(f)

def edit_required(f):
    return require_permission('edit')(f)