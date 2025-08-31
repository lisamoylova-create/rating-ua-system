"""Additional API endpoints for filter options"""
from flask import Blueprint, jsonify
from flask_login import login_required, current_user
from models_full import Company
from app import db
from sqlalchemy import func
import logging

api_filters = Blueprint('api_filters', __name__)

@api_filters.route('/api/regions', methods=['GET'])
def get_regions():
    """API endpoint to get distinct regions"""
    
    try:
        regions = db.session.execute(
            db.select(Company.region_name)
            .distinct()
            .where(Company.region_name.isnot(None))
            .order_by(Company.region_name)
        ).scalars().all()
        
        result = []
        for region in regions:
            result.append({
                'id': region,  # Use region name as ID
                'name': region
            })
        
        return jsonify({'regions': result})
        
    except Exception as e:
        logging.error(f"API error getting regions: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@api_filters.route('/api/kved', methods=['GET'])
def get_kved():
    """API endpoint to get distinct KVED codes"""
    
    try:
        kved_data = db.session.execute(
            db.select(Company.kved_code, Company.kved_description)
            .distinct()
            .where(Company.kved_code.isnot(None))
            .order_by(Company.kved_code)
        ).all()
        
        result = []
        for kved_code, kved_description in kved_data:
            result.append({
                'id': kved_code,  # Use KVED code as ID
                'code': kved_code,
                'description': kved_description or 'Опис відсутній'
            })
        
        return jsonify({'kved': result})
        
    except Exception as e:
        logging.error(f"API error getting KVED: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@api_filters.route('/api/company_sizes', methods=['GET'])
def get_company_sizes():
    """API endpoint to get distinct company sizes"""
    
    try:
        sizes = db.session.execute(
            db.select(Company.company_size_name)
            .distinct()
            .where(Company.company_size_name.isnot(None))
            .order_by(Company.company_size_name)
        ).scalars().all()
        
        result = []
        for size in sizes:
            result.append({
                'id': size,  # Use size name as ID
                'size_name': size
            })
        
        return jsonify({'company_sizes': result})
        
    except Exception as e:
        logging.error(f"API error getting company sizes: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@api_filters.route('/api/selection-stats')
def get_selection_stats():
    """Get selection database statistics"""
    try:
        from flask import session
        
        # Get from session if available
        selection_info = session.get('selection_info')
        if selection_info:
            return jsonify(selection_info)
        
        # Default fallback data
        return jsonify({
            'total_companies': 'Не завантажено',
            'min_personnel': 'Не задано',
            'min_revenue': 'Не задано', 
            'min_profit': 'Не задано',
            'regions_count': 'Всі регіони',
            'kved_count': 'Всі КВЕД коди'
        })
    except Exception as e:
        logging.error(f"Error getting selection stats: {e}")
        return jsonify({'error': 'Failed to load selection stats'}), 500

@api_filters.route('/api/database-stats')
def get_database_stats():
    """Get comprehensive database statistics for infographic"""
    try:
        from flask import session
        from sqlalchemy import func
        
        # Total companies in database
        total_in_database = db.session.execute(
            db.select(func.count(Company.id))
        ).scalar() or 0
        
        # Companies in current selection (from session)
        selected_company_ids = session.get('selected_company_ids', [])
        total_in_selection = len(selected_company_ids)
        
        # Companies with ranking (have ranking assigned)
        companies_with_ranking = db.session.execute(
            db.select(func.count(Company.id))
            .where(Company.ranking.isnot(None))
        ).scalar() or 0
        
        # Selection criteria from session
        selection_criteria = session.get('selection_criteria', 'Критерії не задано')
        
        return jsonify({
            'total_in_database': total_in_database,
            'total_in_selection': total_in_selection,
            'companies_with_ranking': companies_with_ranking,
            'selection_criteria': selection_criteria
        })
        
    except Exception as e:
        logging.error(f"Error getting database stats: {e}")
        return jsonify({'error': 'Failed to load database stats'}), 500


@api_filters.route('/api/stats', methods=['GET'])
@login_required
def get_stats():
    """API endpoint to get basic statistics"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        total_companies = db.session.execute(
            db.select(func.count(Company.id))
        ).scalar() or 0
        
        companies_with_ranking = db.session.execute(
            db.select(func.count(Company.id))
            .where(Company.ranking.isnot(None))
        ).scalar() or 0
        
        total_regions = db.session.execute(
            db.select(func.count(func.distinct(Company.region_name)))
            .where(Company.region_name.isnot(None))
        ).scalar() or 0
        
        total_kved = db.session.execute(
            db.select(func.count(func.distinct(Company.kved_code)))
            .where(Company.kved_code.isnot(None))
        ).scalar() or 0
        
        return jsonify({
            'stats': {
                'total_companies': total_companies,
                'companies_with_ranking': companies_with_ranking,
                'total_regions': total_regions,
                'total_kved': total_kved
            }
        })
        
    except Exception as e:
        logging.error(f"API error getting stats: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@api_filters.route('/api/base_info', methods=['GET'])
@login_required
def get_base_info():
    """API endpoint to get selection base information - alias for selection_base_info"""
    return get_selection_base_info()

@api_filters.route('/api/selection_base_info', methods=['GET'])
@login_required
def get_selection_base_info():
    """API endpoint to get selection base information"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        from flask import session
        # Get selection info from session
        selected_ids = session.get('selected_company_ids', [])
        selection_criteria = session.get('selection_criteria', 'Не створена')
        selection_count = session.get('selection_count', 0)
        
        # Get total statistics
        total_companies = db.session.execute(db.select(func.count(Company.id))).scalar() or 0
        
        # Get companies with rankings
        ranked_count = db.session.execute(
            db.select(func.count(Company.id)).where(Company.ranking.isnot(None))
        ).scalar() or 0
        
        return jsonify({
            'total_companies': total_companies,
            'selection_count': selection_count,
            'ranked_count': ranked_count,
            'selection_criteria': selection_criteria,
            'has_selection': selection_count > 0
        })
        
    except Exception as e:
        logging.error(f"API error getting selection base info: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@api_filters.route('/api/saved_rankings', methods=['GET'])  
@login_required
def get_saved_rankings():
    """API endpoint to get saved rankings"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        # Get rankings - for now just show current ranking info
        companies_with_rankings = db.session.execute(
            db.select(Company)
            .where(Company.ranking.isnot(None))
            .order_by(Company.ranking.asc())
        ).scalars().all()
        
        rankings = []
        if companies_with_rankings:
            # Get info from session if available
            from flask import session
            import datetime
            
            ranking_name = session.get('last_ranking_name', 'Поточний рейтинг')
            ranking_criteria = session.get('last_ranking_criteria', 'revenue')
            ranking_count = session.get('last_ranking_count', len(companies_with_rankings))
            
            # Only show if we have valid ranking data
            if ranking_name and ranking_name != 'Поточний рейтинг' and ranking_count > 0:
                # Translate criteria to Ukrainian
                criteria_names = {
                    'revenue': 'За доходом',
                    'profit': 'За прибутком', 
                    'personnel': 'За персоналом'
                }
                
                rankings.append({
                    'name': ranking_name,
                    'company_count': ranking_count,
                    'created_date': datetime.datetime.now().strftime('%d.%m.%Y'),
                    'criteria': criteria_names.get(ranking_criteria, 'За доходом')
                })
        
        return jsonify({'rankings': rankings})
        
    except Exception as e:
        logging.error(f"API error getting saved rankings: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@api_filters.route('/api/export_ranking', methods=['POST'])
@login_required
def export_ranking():
    """API endpoint to export ranking with all 33 fields"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        from flask import Response
        import csv
        import io
        
        # Get companies with rankings - only those in current ranking
        companies_with_rankings = db.session.execute(
            db.select(Company)
            .where(Company.ranking.isnot(None))
            .order_by(Company.ranking.asc())
        ).scalars().all()
        
        if not companies_with_rankings:
            return jsonify({'error': 'No ranking data to export'}), 400
        
        # Create CSV with all fields
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header with all 33 fields from models_full.py
        headers = [
            'Місце в рейтингу', 'ЄДРПОУ', 'Назва компанії', 'КВЕД код', 'КВЕД опис', 'Регіон',
            'Дохід 2019 (тис. грн)', 'Прибуток 2019 (тис. грн)', 'Персонал 2019', 'Статус актуалізації',
            'Розмір компанії', 'Адреса', 'Телефон', 'Email', 'Веб-сайт',
            'Рік заснування', 'Форма власності', 'ПДВ номер', 'Банк', 'МФО',
            'Рахунок', 'Директор', 'Бухгалтер', 'Основний вид діяльності',
            'Додаткові види діяльності', 'Ліцензії', 'Сертифікати', 'Експорт',
            'Імпорт', 'Примітки', 'Дата створення запису', 'Дата оновлення запису', 'Джерело даних'
        ]
        writer.writerow(headers)
        
        # Write data rows with all available fields
        for company in companies_with_rankings:
            row = [
                company.ranking or '',
                company.edrpou or '',
                company.name or '',
                company.kved_code or '',
                company.kved_description or '',
                company.region_name or '',
                company.revenue_2019 or 0,
                company.profit_2019 or 0,
                company.personnel_2019 or 0,
                company.actualized or 'ні',
                company.company_size_name or '',
                getattr(company, 'address', '') or '',
                getattr(company, 'phone', '') or '',
                getattr(company, 'email_company', '') or '',
                getattr(company, 'website', '') or '',
                getattr(company, 'founding_year', '') or '',
                getattr(company, 'ownership_form', '') or '',
                getattr(company, 'vat_number', '') or '',
                getattr(company, 'bank_name', '') or '',
                getattr(company, 'bank_mfo', '') or '',
                getattr(company, 'bank_account', '') or '',
                getattr(company, 'director_name', '') or '',
                getattr(company, 'accountant_name', '') or '',
                getattr(company, 'main_activity', '') or '',
                getattr(company, 'additional_activities', '') or '',
                getattr(company, 'licenses', '') or '',
                getattr(company, 'certificates', '') or '',
                getattr(company, 'export_countries', '') or '',
                getattr(company, 'import_countries', '') or '',
                getattr(company, 'notes', '') or '',
                company.created_at.strftime('%d.%m.%Y') if company.created_at else '',
                company.updated_at.strftime('%d.%m.%Y') if company.updated_at else '',
                'Рейтинг України 2025'
            ]
            writer.writerow(row)
        
        output.seek(0)
        
        return Response(
            output.getvalue(),
            mimetype='text/csv; charset=utf-8',
            headers={
                'Content-Disposition': f'attachment; filename=rating_ukraine_{len(companies_with_rankings)}_companies.csv',
                'Content-Type': 'text/csv; charset=utf-8'
            }
        )
        
    except Exception as e:
        logging.error(f"API error exporting ranking: {str(e)}")
        return jsonify({'error': 'Export failed'}), 500