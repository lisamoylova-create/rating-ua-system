from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from models_full import Company
from app import db
from sqlalchemy import desc, asc, func
import logging

api = Blueprint('api', __name__)

@api.route('/companies', methods=['GET'])
@login_required
def get_companies():
    """API endpoint to get companies with filtering"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        # Parse query parameters
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 50, type=int), 100)  # Max 100 per page
        
        region_id = request.args.get('region_id', type=int)
        kved_id = request.args.get('kved_id', type=int)
        size_id = request.args.get('size_id', type=int)
        min_employees = request.args.get('min_employees', type=int)
        min_revenue = request.args.get('min_revenue', type=float)
        
        sort_by = request.args.get('sort_by', 'name')
        sort_order = request.args.get('sort_order', 'asc')
        
        # Build query using SQLAlchemy 2.x syntax
        from sqlalchemy import select
        query = select(Company)
        
        # Apply filters
        if region_id:
            query = query.where(Company.region_name == region_id)
        
        if kved_id:
            query = query.where(Company.kved_code == kved_id)
        
        if size_id:
            query = query.where(Company.company_size_name == size_id)
        
        if min_employees:
            query = query.where(Company.personnel_2019 >= min_employees)
        
        if min_revenue:
            query = query.where(Company.revenue_2019 >= min_revenue)
        
        # Apply sorting
        if sort_by == 'revenue':
            query = query.order_by(desc(Company.revenue_2019) if sort_order == 'desc' else asc(Company.revenue_2019))
        elif sort_by == 'profit':
            query = query.order_by(desc(Company.profit_2019) if sort_order == 'desc' else asc(Company.profit_2019))
        elif sort_by == 'personnel':
            query = query.order_by(desc(Company.personnel_2019) if sort_order == 'desc' else asc(Company.personnel_2019))
        elif sort_by == 'ranking':
            query = query.order_by(asc(Company.ranking) if sort_order == 'asc' else desc(Company.ranking))
        else:
            query = query.order_by(asc(Company.name) if sort_order == 'asc' else desc(Company.name))
        
        # Execute query with pagination
        offset = (page - 1) * per_page
        companies = db.session.execute(query.offset(offset).limit(per_page)).scalars().all()
        total = db.session.execute(select(func.count()).select_from(query.subquery())).scalar()
        
        # Format response
        companies_data = []
        for company in companies:
            company_data = {
                'id': company.id,
                'edrpou': company.edrpou,
                'name': company.name,
                'phone': company.phone,
                'address': company.address,
                'personnel_2019': company.personnel_2019,
                'ranking': company.ranking,
                'region_name': company.region_name,
                'kved_code': company.kved_code,
                'kved_description': company.kved_description,
                'company_size_name': company.company_size_name,
                'revenue_2019': float(company.revenue_2019) if company.revenue_2019 else None,
                'profit_2019': float(company.profit_2019) if company.profit_2019 else None,
                'actualized': company.actualized or 'ні'
            }
            companies_data.append(company_data)
        
        # Calculate pagination
        pages = (total + per_page - 1) // per_page
        has_next = page < pages
        has_prev = page > 1
        
        response = {
            'companies': companies_data,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': pages,
                'has_next': has_next,
                'has_prev': has_prev
            }
        }
        
        return jsonify(response)
        
    except Exception as e:
        logging.error(f"API error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

# Note: Region, KVED, and CompanySize endpoints moved to api_endpoints.py
# This avoids import errors with non-existent model classes

@api.route('/selection_base')
@login_required
def get_selection_base():
    """Get companies in the active selection base"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        selection_base = SelectionBase.query.filter_by(is_active=True).first()
        if not selection_base:
            return jsonify({"companies": [], "count": 0})
        
        companies_query = db.session.query(Company).join(SelectionCompany).filter(
            SelectionCompany.selection_base_id == selection_base.id
        ).outerjoin(Financial).outerjoin(Region).outerjoin(Kved)
        
        companies = companies_query.all()
        
        companies_data = []
        for company in companies:
            company_data = {
                "id": company.id,
                "edrpou": company.edrpou,
                "name": company.name,
                "address": company.address,
                "phone": company.phone,
                "personnel_2019": company.personnel_2019,
                "region": {
                    "id": company.region.id,
                    "name": company.region.name
                } if company.region else None,
                "kved": {
                    "id": company.kved.id,
                    "code": company.kved.code,
                    "description": company.kved.description
                } if company.kved else None,
                "financial": {
                    "revenue": float(company.financials[0].revenue) if company.financials and len(company.financials) > 0 else None,
                    "profit": float(company.financials[0].profit) if company.financials and len(company.financials) > 0 else None
                } if company.financials and len(company.financials) > 0 else None
            }
            companies_data.append(company_data)
        
        return jsonify({
            "companies": companies_data,
            "count": len(companies_data),
            "selection_base": {
                "id": selection_base.id,
                "name": selection_base.name,
                "created_at": selection_base.created_at.isoformat()
            }
        })
        
    except Exception as e:
        logging.error(f"API error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@api.route('/selection_base_info')
@login_required
def get_selection_base_info():
    """Get information about the active selection base"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        selection_base = SelectionBase.query.filter_by(is_active=True).first()
        if not selection_base:
            return jsonify({"count": 0, "created_at": None, "criteria": None})
        
        criteria_parts = []
        if selection_base.min_employees > 0:
            criteria_parts.append(f"Працівників ≥ {selection_base.min_employees}")
        if selection_base.min_revenue > 0:
            criteria_parts.append(f"Дохід ≥ {selection_base.min_revenue:,.0f} грн")
        if selection_base.min_profit is not None:
            criteria_parts.append(f"Прибуток ≥ {selection_base.min_profit:,.0f} грн")
        
        return jsonify({
            "count": selection_base.companies_count,
            "created_at": selection_base.created_at.isoformat(),
            "criteria": ", ".join(criteria_parts) if criteria_parts else "Основні критерії"
        })
        
    except Exception as e:
        logging.error(f"API error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@api.route('/saved_rankings')
@login_required
def get_saved_rankings():
    """Get list of saved rankings"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        rankings = Ranking.query.order_by(desc(Ranking.created_at)).limit(10).all()
        
        rankings_data = []
        for ranking in rankings:
            criteria_parts = []
            if ranking.kved_filters:
                criteria_parts.append("КВЕД")
            if ranking.region_filters:
                criteria_parts.append("Регіон")
            if ranking.size_filters:
                criteria_parts.append("Розмір")
            criteria_parts.append(f"Сортування: {ranking.sort_criteria}")
            
            ranking_data = {
                "id": ranking.id,
                "name": ranking.name,
                "companies_count": ranking.companies_count,
                "created_at": ranking.created_at.isoformat(),
                "criteria": ", ".join(criteria_parts)
            }
            rankings_data.append(ranking_data)
        
        return jsonify({"rankings": rankings_data})
        
    except Exception as e:
        logging.error(f"API error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@api.route('/stats', methods=['GET'])
@login_required
def get_stats():
    """API endpoint to get system statistics"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        stats = {
            'total_companies': Company.query.count(),
            'total_regions': Region.query.count(),
            'total_kved': Kved.query.count(),
            'total_company_sizes': CompanySize.query.count(),
            'companies_with_ranking': Company.query.filter(Company.ranking.isnot(None)).count(),
            'companies_with_financials': db.session.query(Company.id).join(Financial).distinct().count()
        }
        return jsonify({'stats': stats})
    except Exception as e:
        logging.error(f"API error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@api.route('/ranking/<int:ranking_id>')
@login_required
def get_ranking(ranking_id):
    """Get specific ranking with companies"""
    if not current_user.has_permission('view'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        ranking = Ranking.query.get_or_404(ranking_id)
        
        # Get ranking companies with full details
        ranking_companies = db.session.query(RankingCompany, Company, Region, Kved, Financial).join(
            Company, RankingCompany.company_id == Company.id
        ).outerjoin(
            Region, Company.region_id == Region.id
        ).outerjoin(
            Kved, Company.kved_id == Kved.id
        ).outerjoin(
            Financial, Company.id == Financial.company_id
        ).filter(
            RankingCompany.ranking_id == ranking_id
        ).order_by(RankingCompany.rank_position).all()
        
        companies_data = []
        for rank_company, company, region, kved, financial in ranking_companies:
            company_data = {
                "rank_position": rank_company.rank_position,
                "sort_value": float(rank_company.sort_value) if rank_company.sort_value else None,
                "company": {
                    "id": company.id,
                    "edrpou": company.edrpou,
                    "name": company.name,
                    "address": company.address,
                    "phone": company.phone,
                    "personnel_2019": company.personnel_2019,
                    "region": {
                        "id": region.id,
                        "name": region.name
                    } if region else None,
                    "kved": {
                        "id": kved.id,
                        "code": kved.code,
                        "description": kved.description
                    } if kved else None,
                    "financial": {
                        "revenue": float(financial.revenue) if financial and financial.revenue else None,
                        "profit": float(financial.profit) if financial and financial.profit else None
                    } if financial else None
                }
            }
            companies_data.append(company_data)
        
        ranking_data = {
            "id": ranking.id,
            "name": ranking.name,
            "sort_criteria": ranking.sort_criteria,
            "companies_count": ranking.companies_count,
            "created_at": ranking.created_at.isoformat()
        }
        
        return jsonify({
            "ranking": ranking_data,
            "companies": companies_data
        })
        
    except Exception as e:
        logging.error(f"API error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500
