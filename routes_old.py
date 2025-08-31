import os
import logging
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, current_app
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from data_processor_full import process_first_file, process_second_file
# Temporarily disable old data_processor imports
from file_merger import ExcelFileMerger
from models_full import Company, SelectionBase, SelectionCompany, Ranking, RankingCompany
from app import db
import pandas as pd
from sqlalchemy import desc, asc, select, func

main = Blueprint('main', __name__)

ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

# Глобальний об'єкт для злиття файлів
file_merger = ExcelFileMerger()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@main.route('/')
def index():
    if current_user.is_authenticated:
        try:
            # Get basic statistics with safe queries
            total_companies = db.session.execute(db.select(db.func.count(Company.id))).scalar() or 0
            
            # Use simpler count for distinct values
            region_count_query = db.session.execute(
                db.text("SELECT COUNT(DISTINCT region_name) FROM companies WHERE region_name IS NOT NULL")
            ).scalar() or 0
            
            kved_count_query = db.session.execute(
                db.text("SELECT COUNT(DISTINCT kved_code) FROM companies WHERE kved_code IS NOT NULL")
            ).scalar() or 0
            
            # Recent uploads with error handling
            recent_companies = []
            try:
                recent_companies = db.session.execute(
                    db.select(Company).order_by(desc(Company.created_at)).limit(5)
                ).scalars().all()
            except Exception as e:
                logging.error(f"Error getting recent companies: {e}")
            
            stats = {
                'total_companies': total_companies,
                'total_regions': region_count_query,
                'total_kved': kved_count_query,
                'recent_companies': recent_companies
            }
            return render_template('index.html', stats=stats)
        except Exception as e:
            logging.error(f"Error in index route: {e}")
            # Return basic template without stats
            return render_template('index.html', stats={
                'total_companies': 0,
                'total_regions': 0,
                'total_kved': 0,
                'recent_companies': []
            })
    else:
        return redirect(url_for('auth.login'))

@main.route('/upload', methods=['GET', 'POST'])
@login_required
def upload():
    if not current_user.has_permission('upload'):
        flash('У вас немає прав для завантаження файлів.', 'danger')
        return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        # Check if the post request has the file part
        if 'file' not in request.files:
            flash('Файл не обрано', 'danger')
            return redirect(request.url)
        
        file = request.files['file']
        merge_file = request.files.get('merge_file')
        
        if file.filename == '':
            flash('Файл не обрано', 'danger')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            try:
                filename = secure_filename(file.filename or 'unknown.xlsx')
                filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
                file.save(filepath)
                
                # Determine action based on form submission
                action = request.form.get('action', 'upload')
                df = pd.read_excel(filepath)
                
                if action == 'upload':
                    # Process as first file (основний файл)
                    success_count, error_count = process_first_file(df)
                    if success_count > 0:
                        flash(f'✅ Основний файл завантажено: {success_count} компаній додано, {error_count} помилок.', 'success')
                        flash(f'📋 Перейдіть до списку компаній для перевірки, потім поверніться для актуалізації (Крок 2).', 'info')
                    else:
                        flash(f'❌ Помилка завантаження основного файлу: {error_count} помилок. Перевірте формат файлу.', 'danger')
                        return redirect(request.url)
                elif action == 'actualize':
                    # Process as second file for actualization
                    success_count, error_count = process_second_file(df)
                    if success_count > 0:
                        flash(f'✅ Актуалізація завершена: {success_count} компаній оновлено, {error_count} помилок.', 'success')
                    else:
                        flash(f'❌ Помилка актуалізації: {error_count} помилок. Перевірте формат файлу та наявність компаній.', 'danger')
                        return redirect(request.url)
                
                # Remove merge file processing logic as we now use separate buttons
                
                # Clean up uploaded file
                os.remove(filepath)
                
                return redirect(url_for('main.companies'))
                
            except Exception as e:
                logging.error(f"Error processing file: {str(e)}")
                flash(f'Помилка обробки файлу: {str(e)}', 'danger')
                return redirect(request.url)
        else:
            flash('Недозволений тип файлу. Дозволені: xlsx, xls, csv', 'danger')
            return redirect(request.url)
    
    return render_template('upload.html')

@main.route('/companies')
@login_required
def companies():
    if not current_user.has_permission('view'):
        flash('У вас немає прав для перегляду компаній.', 'danger')
        return redirect(url_for('main.index'))
    
    page = request.args.get('page', 1, type=int)
    per_page = 50
    
    # Build query with optional filters
    query = db.select(Company)
    
    # Filter by region name
    region_name = request.args.get('region_name', type=str)
    if region_name:
        query = query.where(Company.region_name == region_name)
    
    # Filter by KVED code
    kved_code = request.args.get('kved_code', type=str)
    if kved_code:
        query = query.where(Company.kved_code == kved_code)
    
    # Sorting
    sort_by = request.args.get('sort_by', 'name')
    sort_order = request.args.get('sort_order', 'asc')
    
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
    
    # Get companies for pagination manually
    companies = db.session.execute(query).scalars().all()
    
    # Get unique values for filters (filter out None values)
    regions = db.session.execute(
        db.select(Company.region_name).distinct().where(Company.region_name.isnot(None))
    ).scalars().all()
    kveds = db.session.execute(
        db.select(Company.kved_code).distinct().where(Company.kved_code.isnot(None))
    ).scalars().all()
    sizes = db.session.execute(
        db.select(Company.company_size_name).distinct().where(Company.company_size_name.isnot(None))
    ).scalars().all()
    
    return render_template('companies.html', 
                         companies=companies,
                         regions=regions, 
                         kveds=kveds, 
                         sizes=sizes,
                         current_filters={
                             'region_name': region_name,
                             'kved_code': kved_code,
                             'sort_by': sort_by,
                             'sort_order': sort_order
                         })

@main.route('/filter', methods=['GET', 'POST'])
@login_required
def filter_companies_route():
    if not current_user.has_permission('edit'):
        flash('У вас немає прав для фільтрації компаній.', 'danger')
        return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        # Step 1: Primary filters
        min_employees = request.form.get('min_employees', type=int, default=0)
        min_revenue = request.form.get('min_revenue', type=int, default=0)
        min_profit_str = request.form.get('min_profit', '').strip()
        min_profit = float(min_profit_str) if min_profit_str else None
        
        # Step 2: Secondary filters
        region_filter = request.form.getlist('region_filter')
        kved_filter = request.form.getlist('kved_filter')
        size_filter = request.form.getlist('size_filter')
        
        # Convert to integers if values exist
        region_ids = [int(r) for r in region_filter if r] if region_filter else None
        kved_ids = [int(k) for k in kved_filter if k] if kved_filter else None
        size_ids = [int(s) for s in size_filter if s] if size_filter else None
        
        # Step 3: Sorting criteria and metadata
        sort_criteria = request.form.get('sort_criteria', 'revenue')
        apply_regional_filter = request.form.get('apply_regional_filter') == 'on'
        year_source = request.form.get('year_source', '2025')
        ranking_name = request.form.get('ranking_name', '')
        
        try:
            # Temporarily disable filtering functionality - will implement later
            flash('Фільтрація тимчасово недоступна. Завершується налаштування бази даних.', 'warning')
            return redirect(url_for('main.companies'))
            
            # Build detailed message about applied filters
            filter_details = []
            if min_employees > 0:
                filter_details.append(f"працівників ≥ {min_employees}")
            if min_revenue > 0:
                filter_details.append(f"дохід ≥ {min_revenue:,.0f} грн")
            if min_profit is not None:
                filter_details.append(f"прибуток ≥ {min_profit:,.0f} грн")
            if region_ids:
                filter_details.append(f"{len(region_ids)} регіон(ів)")
            if kved_ids:
                filter_details.append(f"{len(kved_ids)} КВЕД код(ів)")
            if size_ids:
                filter_details.append(f"{len(size_ids)} розмір(ів)")
            
            filter_summary = ", ".join(filter_details) if filter_details else "без обмежень"
            
            flash(f'Послідовна фільтрація завершена ({filter_summary}). {filtered_count} компаній у фінальному рейтингу за критерієм "{sort_criteria}".', 'success')
            return redirect(url_for('main.companies', sort_by='ranking', sort_order='asc'))
            
        except Exception as e:
            logging.error(f"Error filtering companies: {str(e)}")
            flash(f'Помилка фільтрації: {str(e)}', 'danger')
    
    # Load filter options for GET request - using simplified structure
    regions = db.session.execute(
        db.select(Company.region_name).distinct().where(Company.region_name.isnot(None))
    ).scalars().all()
    kveds = db.session.execute(
        db.select(Company.kved_code).distinct().where(Company.kved_code.isnot(None))
    ).scalars().all()
    sizes = db.session.execute(
        db.select(Company.company_size_name).distinct().where(Company.company_size_name.isnot(None))
    ).scalars().all()
    
    return render_template('filter.html', regions=regions, kveds=kveds, sizes=sizes)

@main.route('/export')
@login_required
def export():
    if not current_user.has_permission('view'):
        flash('У вас немає прав для експорту даних.', 'danger')
        return redirect(url_for('main.index'))
    
    try:
        # Get filtered companies (those with ranking)
        companies = Company.query.filter(Company.ranking.isnot(None)).order_by(Company.ranking).all()
        
        if not companies:
            flash('Немає даних для експорту. Спочатку проведіть фільтрацію.', 'warning')
            return redirect(url_for('main.companies'))
        
        export_path = export_to_csv(companies)
        
        return send_file(export_path, 
                        as_attachment=True, 
                        download_name='filtered_companies.csv',
                        mimetype='text/csv')
        
    except Exception as e:
        logging.error(f"Error exporting data: {str(e)}")
        flash(f'Помилка експорту: {str(e)}', 'danger')
        return redirect(url_for('main.companies'))

@main.route('/selection_for_ranking', methods=['GET', 'POST'])
@login_required
def selection_for_ranking():
    if not current_user.has_permission('edit'):
        flash('У вас немає прав для створення бази для ранжування.', 'danger')
        return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        min_employees = request.form.get('min_employees', type=int, default=0)
        min_revenue = request.form.get('min_revenue', type=int, default=0)
        min_profit_str = request.form.get('min_profit', '').strip()
        min_profit = float(min_profit_str) if min_profit_str else None
        
        try:
            # Get companies that meet criteria
            query = db.session.query(Company).join(Financial)
            
            if min_employees > 0:
                query = query.filter(Company.personnel_2019 >= min_employees)
            if min_revenue > 0:
                query = query.filter(Financial.revenue >= min_revenue)
            if min_profit is not None:
                query = query.filter(Financial.profit >= min_profit)
            
            companies = query.all()
            
            # Create new selection base
            # Deactivate previous selection bases
            SelectionBase.query.update({SelectionBase.is_active: False})
            
            # Create new selection base
            selection_base = SelectionBase()
            selection_base.name = f'База для ранжування ({len(companies)} компаній)'
            selection_base.min_employees = min_employees
            selection_base.min_revenue = min_revenue
            selection_base.min_profit = min_profit
            selection_base.companies_count = len(companies)
            selection_base.is_active = True
            db.session.add(selection_base)
            db.session.flush()
            
            # Add companies to selection base
            for company in companies:
                selection_company = SelectionCompany()
                selection_company.selection_base_id = selection_base.id
                selection_company.company_id = company.id
                db.session.add(selection_company)
            
            db.session.commit()
            
            flash(f'База для ранжування створена. Відібрано {len(companies)} компаній за критеріями: працівників ≥ {min_employees}, дохід ≥ {min_revenue:,.0f} грн{f", прибуток ≥ {min_profit:,.0f} грн" if min_profit else ""}.', 'success')
            
        except Exception as e:
            db.session.rollback()
            logging.error(f"Error creating selection base: {str(e)}")
            flash(f'Помилка створення бази: {str(e)}', 'danger')
    
    return render_template('selection_for_ranking.html')

@main.route('/ranking', methods=['GET', 'POST'])
@login_required
def ranking():
    if not current_user.has_permission('edit'):
        flash('У вас немає прав для створення рейтингу.', 'danger')
        return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        # Get active selection base
        selection_base = SelectionBase.query.filter_by(is_active=True).first()
        if not selection_base:
            flash('Спочатку створіть базу для ранжування.', 'warning')
            return redirect(url_for('main.selection_for_ranking'))
        
        # Get form data
        kved_filter = request.form.getlist('kved_filter')
        region_filter = request.form.getlist('region_filter')
        size_filter = request.form.getlist('size_filter')
        sort_criteria = request.form.get('sort_criteria', 'revenue')
        ranking_name = request.form.get('ranking_name', f'Рейтинг за {sort_criteria}')
        
        try:
            # Get companies from selection base
            companies_query = db.session.query(Company).join(SelectionCompany).filter(
                SelectionCompany.selection_base_id == selection_base.id
            ).join(Financial)
            
            # Apply filters
            if kved_filter:
                kved_ids = [int(k) for k in kved_filter if k]
                companies_query = companies_query.filter(Company.kved_id.in_(kved_ids))
            
            if region_filter:
                region_ids = [int(r) for r in region_filter if r]
                companies_query = companies_query.filter(Company.region_id.in_(region_ids))
            
            if size_filter:
                size_ids = [int(s) for s in size_filter if s]
                companies_query = companies_query.filter(Company.company_size_id.in_(size_ids))
            
            companies = companies_query.all()
            
            if not companies:
                flash('Жодна компанія не відповідає обраним фільтрам.', 'warning')
                return render_template('ranking.html')
            
            # Sort companies
            if sort_criteria == 'revenue':
                companies.sort(key=lambda c: (c.financials[0].revenue if c.financials and len(c.financials) > 0 else 0) or 0, reverse=True)
            elif sort_criteria == 'profit':
                companies.sort(key=lambda c: (c.financials[0].profit if c.financials and len(c.financials) > 0 else 0) or 0, reverse=True)
            elif sort_criteria == 'personnel':
                companies.sort(key=lambda c: (c.personnel_2019 or 0), reverse=True)
            
            # Create ranking
            ranking = Ranking()
            ranking.name = ranking_name
            ranking.selection_base_id = selection_base.id
            ranking.sort_criteria = sort_criteria
            ranking.region_filters = ','.join(region_filter) if region_filter else None
            ranking.kved_filters = ','.join(kved_filter) if kved_filter else None
            ranking.size_filters = ','.join(size_filter) if size_filter else None
            ranking.companies_count = len(companies)
            db.session.add(ranking)
            db.session.flush()
            
            # Add companies to ranking
            for position, company in enumerate(companies, 1):
                if sort_criteria == 'revenue':
                    sort_value = (company.financials[0].revenue if company.financials and len(company.financials) > 0 else 0) or 0
                elif sort_criteria == 'profit':
                    sort_value = (company.financials[0].profit if company.financials and len(company.financials) > 0 else 0) or 0
                else:  # personnel
                    sort_value = company.personnel_2019 or 0
                
                ranking_company = RankingCompany()
                ranking_company.ranking_id = ranking.id
                ranking_company.company_id = company.id
                ranking_company.rank_position = position
                ranking_company.sort_value = sort_value
                db.session.add(ranking_company)
            
            db.session.commit()
            
            flash(f'Рейтинг "{ranking_name}" створено з {len(companies)} компаніями.', 'success')
            
            # Pass ranking data to template for immediate display
            ranking_data = {
                'id': ranking.id,
                'name': ranking.name,
                'sort_criteria': ranking.sort_criteria,
                'companies_count': len(companies),
                'created_at': ranking.created_at.isoformat()
            }
            
            companies_data = []
            for position, company in enumerate(companies, 1):
                if sort_criteria == 'revenue':
                    sort_value = (company.financials[0].revenue if company.financials and len(company.financials) > 0 else 0) or 0
                elif sort_criteria == 'profit':
                    sort_value = (company.financials[0].profit if company.financials and len(company.financials) > 0 else 0) or 0
                else:  # personnel
                    sort_value = company.personnel_2019 or 0
                
                company_data = {
                    'rank_position': position,
                    'sort_value': float(sort_value),
                    'company': {
                        'id': company.id,
                        'edrpou': company.edrpou,
                        'name': company.name,
                        'address': company.address,
                        'phone': company.phone,
                        'personnel_2019': company.personnel_2019,
                        'region': {
                            'id': company.region.id,
                            'name': company.region.name
                        } if company.region else None,
                        'kved': {
                            'id': company.kved.id,
                            'code': company.kved.code,
                            'description': company.kved.description
                        } if company.kved else None,
                        'financial': {
                            'revenue': float(company.financials[0].revenue) if company.financials and len(company.financials) > 0 and company.financials[0].revenue else None,
                            'profit': float(company.financials[0].profit) if company.financials and len(company.financials) > 0 and company.financials[0].profit else None
                        } if company.financials and len(company.financials) > 0 else None
                    }
                }
                companies_data.append(company_data)
            
            return render_template('ranking.html', ranking_created=ranking_data, ranking_companies=companies_data)
            
        except Exception as e:
            db.session.rollback()
            logging.error(f"Error creating ranking: {str(e)}")
            flash(f'Помилка створення рейтингу: {str(e)}', 'danger')
    
    return render_template('ranking.html')

@main.route('/export_csv')
@login_required
def export_csv():
    if not current_user.has_permission('view'):
        flash('У вас немає прав для експорту даних.', 'danger')
        return redirect(url_for('main.index'))
    
    try:
        # Get latest ranking
        ranking = Ranking.query.order_by(desc(Ranking.created_at)).first()
        if not ranking:
            flash('Спочатку створіть рейтинг.', 'warning')
            return redirect(url_for('main.ranking'))
        
        # Get ranking companies
        ranking_companies = db.session.query(RankingCompany, Company, Financial).join(
            Company, RankingCompany.company_id == Company.id
        ).outerjoin(
            Financial, Company.id == Financial.company_id
        ).filter(
            RankingCompany.ranking_id == ranking.id
        ).order_by(RankingCompany.rank_position).all()
        
        data = []
        for rank_company, company, financial in ranking_companies:
            row = {
                'Рейтинг': rank_company.rank_position,
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
                'Розмір компанії': company.company_size.size_name if company.company_size else '',
                'Источник': company.source or '',
                'ТОП': company.top_count or '',
                'Загальна к-ть': company.total_count or '',
                'Актуалізовано': company.actualized or ''
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        export_path = f'ranking_{ranking.id}_export.csv'
        df.to_csv(export_path, index=False, encoding='utf-8')
        
        return send_file(export_path, 
                        as_attachment=True, 
                        download_name=f'{ranking.name}_ranking.csv',
                        mimetype='text/csv')
        
    except Exception as e:
        logging.error(f"Error exporting ranking: {str(e)}")
        flash(f'Помилка експорту: {str(e)}', 'danger')
        return redirect(url_for('main.ranking'))

@main.route('/export_pdf')
@login_required
def export_pdf():
    if not current_user.has_permission('view'):
        flash('У вас немає прав для експорту даних.', 'danger')
        return redirect(url_for('main.index'))
    
    try:
        from reportlab.lib.pagesizes import letter, A4
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER, TA_LEFT
        from io import BytesIO
        import os
        
        # Get latest ranking
        ranking = Ranking.query.order_by(desc(Ranking.created_at)).first()
        if not ranking:
            flash('Спочатку створіть рейтинг.', 'warning')
            return redirect(url_for('main.ranking'))
        
        # Get ranking companies
        ranking_companies = db.session.query(RankingCompany, Company, Region, Kved, Financial).join(
            Company, RankingCompany.company_id == Company.id
        ).outerjoin(
            Region, Company.region_id == Region.id
        ).outerjoin(
            Kved, Company.kved_id == Kved.id
        ).outerjoin(
            Financial, Company.id == Financial.company_id
        ).filter(
            RankingCompany.ranking_id == ranking.id
        ).order_by(RankingCompany.rank_position).all()
        
        # Create PDF in memory
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=1*inch)
        
        # Container for the 'Flowable' objects
        elements = []
        
        # Get styles
        styles = getSampleStyleSheet()
        
        # Custom styles
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=18,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        )
        
        subtitle_style = ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Normal'],
            fontSize=12,
            spaceAfter=20,
            alignment=TA_CENTER,
            textColor=colors.gray
        )
        
        # Title with company logo placeholder
        title = Paragraph(f"<b>РЕЙТИНГ КОМПАНІЙ УКРАЇНИ</b>", title_style)
        elements.append(title)
        
        subtitle = Paragraph(f"{ranking.name}<br/>Створено: {ranking.created_at.strftime('%d.%m.%Y')}", subtitle_style)
        elements.append(subtitle)
        
        elements.append(Spacer(1, 0.5*inch))
        
        # Prepare table data
        table_data = [['Рейтинг', 'Код ЄДРПОУ', 'Назва компанії']]
        
        for rank_company, company, region, kved, financial in ranking_companies:
            table_data.append([
                str(rank_company.rank_position),
                company.edrpou or '',
                company.name or ''
            ])
        
        # Create table
        table = Table(table_data, colWidths=[1*inch, 1.5*inch, 4*inch])
        
        # Table style
        table.setStyle(TableStyle([
            # Header row
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            
            # Body rows
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),  # Ranking column center
            ('ALIGN', (1, 1), (1, -1), 'CENTER'),  # EDRPOU column center
            ('ALIGN', (2, 1), (2, -1), 'LEFT'),    # Company name left
            
            # Grid
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            
            # Alternate row colors
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey]),
        ]))
        
        elements.append(table)
        
        # Footer
        elements.append(Spacer(1, 0.5*inch))
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=10,
            alignment=TA_CENTER,
            textColor=colors.gray
        )
        
        criteria_parts = []
        if ranking.kved_filters:
            criteria_parts.append("КВЕД")
        if ranking.region_filters:
            criteria_parts.append("Регіон") 
        if ranking.size_filters:
            criteria_parts.append("Розмір")
        criteria_text = f"Критерії фільтрації: {', '.join(criteria_parts)}" if criteria_parts else "Без додаткових фільтрів"
        
        footer = Paragraph(f"Кількість компаній: {len(ranking_companies)}<br/>{criteria_text}<br/>Сортування: {ranking.sort_criteria}", footer_style)
        elements.append(footer)
        
        # Build PDF
        doc.build(elements)
        
        # Get PDF data
        pdf_data = buffer.getvalue()
        buffer.close()
        
        # Create response
        from flask import Response
        import urllib.parse
        
        # Safely encode filename for HTTP header
        safe_filename = f"ranking_{ranking.id}.pdf"
        encoded_filename = urllib.parse.quote(ranking.name.encode('utf-8')) if ranking.name else "rating"
        
        response = Response(pdf_data, content_type='application/pdf')
        response.headers['Content-Disposition'] = f'attachment; filename="{safe_filename}"; filename*=UTF-8\'\'{encoded_filename}.pdf'
        
        return response
        
    except ImportError as e:
        logging.error(f"Missing PDF library: {str(e)}")
        flash('Помилка: бібліотека для створення PDF не встановлена.', 'danger')
        return redirect(url_for('main.ranking'))
    except Exception as e:
        logging.error(f"Error creating PDF: {str(e)}")
        flash(f'Помилка створення PDF: {str(e)}', 'danger')
        return redirect(url_for('main.ranking'))




