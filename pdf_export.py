#!/usr/bin/env python3
"""
Експорт рейтингів компаній в PDF з фірмовим бланком
"""

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_CENTER
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from app import app, db
import os
from datetime import datetime

def register_fonts():
    """Register fonts for Ukrainian text"""
    try:
        # Use system fonts that support Cyrillic
        pdfmetrics.registerFont(TTFont('DejaVu', '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'))
        pdfmetrics.registerFont(TTFont('DejaVu-Bold', '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'))
        return True
    except:
        return False

def create_pdf_export(ranking_id, output_path):
    """
    Створює PDF експорт рейтингу з фірмовим бланком
    """
    with app.app_context():
        # Отримуємо дані рейтингу
        ranking = db.session.execute(db.text("""
            SELECT r.id, r.name, r.created_at, 
                   COUNT(rc.id) as company_count
            FROM rankings r
            LEFT JOIN ranking_companies rc ON r.id = rc.ranking_id
            WHERE r.id = :ranking_id
            GROUP BY r.id, r.name, r.created_at
        """), {'ranking_id': ranking_id}).fetchone()
        
        if not ranking:
            return False, "Рейтинг не знайдено"
        
        # Отримуємо компанії рейтингу з потрібними полями
        companies = db.session.execute(db.text("""
            SELECT c.edrpou, c.name, c.kved_code, c.kved_description,
                   rc.position as rank_position
            FROM ranking_companies rc
            JOIN companies c ON rc.company_id = c.id
            WHERE rc.ranking_id = :ranking_id
            ORDER BY rc.position ASC
        """), {'ranking_id': ranking_id}).fetchall()
        
        if not companies:
            return False, "Компанії в рейтингу не знайдено"
        
        # Створюємо PDF документ
        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=1*cm,
            leftMargin=1*cm,
            topMargin=1*cm,
            bottomMargin=1*cm
        )
        
        story = []
        styles = getSampleStyleSheet()
        
        # Реєструємо шрифти
        font_registered = register_fonts()
        
        # Створюємо стилі для українського тексту
        if font_registered:
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Title'],
                fontName='DejaVu-Bold',
                fontSize=16,
                alignment=1,  # Center
                spaceAfter=20,
                textColor=colors.HexColor('#1f4e79')
            )
            
            normal_style = ParagraphStyle(
                'CustomNormal',
                parent=styles['Normal'],
                fontName='DejaVu',
                fontSize=10
            )
        else:
            title_style = styles['Title']
            normal_style = styles['Normal']
        
        # Додаємо тільки фірмовий логотип
        logo_path = 'attached_assets/shapka_liga_1756405895606.png'
        if os.path.exists(logo_path):
            try:
                logo = Image(logo_path, width=18*cm, height=3*cm, hAlign='CENTER')
                story.append(logo)
                story.append(Spacer(1, 15))
            except:
                pass
        
        # Створюємо стиль для заголовків з переносом
        header_style = ParagraphStyle(
            'HeaderText',
            fontName='DejaVu-Bold' if font_registered else 'Helvetica-Bold',
            fontSize=9,
            alignment=TA_CENTER,
            wordWrap='CJK'
        )
        
        # Створюємо таблицю компаній з 5 колонками у вказаному порядку
        table_data = [
            ['Код ЄДРПОУ', 'Назва компанії', Paragraph('Місце в рейтингу', header_style), 'Код КВЕД', 'Галузь']
        ]
        
        # Компактний стиль для переносу слів
        wrap_style = ParagraphStyle(
            'WrapText',
            fontName='DejaVu' if font_registered else 'Helvetica',
            fontSize=8,
            alignment=TA_CENTER,
            wordWrap='CJK'  # Перенос по словам
        )
        
        for company in companies:
            # Обгортаємо назву компанії та KVED в Paragraph
            company_name = Paragraph(company.name if company.name else '', wrap_style)
            kved_description = Paragraph(company.kved_description if company.kved_description else '', wrap_style)
            
            table_data.append([
                str(company.edrpou) if company.edrpou else '',
                company_name,  # Paragraph для переносу
                str(company.rank_position),
                company.kved_code if company.kved_code else '',
                kved_description  # Paragraph для переносу
            ])
        
        # Створюємо таблицю з 5 колонками
        table = Table(table_data, colWidths=[2.5*cm, 5*cm, 2.5*cm, 2*cm, 6*cm])
        
        table_style = [
            # Зелена шапка таблиці
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#228B22')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            
            # Синя підсвітка колонки "Місце в рейтингу" (3-тя колонка, індекс 2)
            ('BACKGROUND', (2, 1), (2, -1), colors.HexColor('#87CEEB')),
            
            # Весь текст по центру
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            
            # Перенос слів вже обробляється Paragraph об'єктами
            
            # Шрифти
            ('FONTNAME', (0, 0), (-1, 0), 'DejaVu-Bold' if font_registered else 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTNAME', (0, 1), (-1, -1), 'DejaVu' if font_registered else 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            
            # Паддинги
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
            
            # Сітка таблиці
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            
            # Білий фон для даних
            ('BACKGROUND', (0, 1), (1, -1), colors.white),
            ('BACKGROUND', (3, 1), (-1, -1), colors.white)
        ]
        
        table.setStyle(TableStyle(table_style))
        story.append(table)
        
        try:
            doc.build(story)
            return True, f"PDF створено: {output_path}"
        except Exception as e:
            return False, f"Помилка створення PDF: {str(e)}"

def export_ranking_to_pdf(ranking_id):
    """
    Експортує рейтинг в PDF файл
    """
    # Створюємо директорію для експорту
    export_dir = 'static/exports'
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)
    
    # Генеруємо ім'я файлу
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'ranking_{ranking_id}_{timestamp}.pdf'
    output_path = os.path.join(export_dir, filename)
    
    success, message = create_pdf_export(ranking_id, output_path)
    
    if success:
        return True, filename, message
    else:
        return False, None, message

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        ranking_id = int(sys.argv[1])
        success, filename, message = export_ranking_to_pdf(ranking_id)
        print(f"Статус: {'Успіх' if success else 'Помилка'}")
        print(f"Повідомлення: {message}")
        if success:
            print(f"Файл: {filename}")
    else:
        print("Використання: python pdf_export.py <ranking_id>")