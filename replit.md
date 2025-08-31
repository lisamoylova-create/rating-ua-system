# Overview

This is "Рейтинг.UA" - a Flask-based web application for processing and managing Ukrainian company data from Excel files. The system provides comprehensive data processing capabilities including file upload, filtering, sorting, ranking, and export functionality. It features a role-based authentication system with different permission levels and a responsive Ukrainian-language interface.

# User Preferences

Preferred communication style: Simple, everyday language.
Data Processing: Use ЄДРПОУ as unique key - update existing companies, create new if not found.
Architecture: Simple two-file system: File 1 (basic data) + File 2 (additional data) → Complete SQL database with 33 columns.
CSV Export: Include all company fields (33 columns) plus "Місце в рейтингу" and "Источник" field with format "Україна [year]".
Infographic Labels: "В рейтингах" instead of "Проранжовано", "Зроблено" instead of "% проранжовано".
Version Control: Replit's File History and Checkpoints interface is difficult to navigate - user finds it "кошмар який, так складно". Prefers simpler backup methods like tar.gz archives or manual file copies (ranking_old.html approach).
UX Design: Prefers compact interfaces with efficient space usage. KVED fields should show only codes without descriptions. Ranking form should be organized with related fields grouped together (criteria + sorting, name on separate line).
**COMPLETED 2025-08-27**: Multiple ranking history system implemented - companies can have different rankings with different criteria, all stored in company_ranking_history table with source tracking.
**TESTING 2025-08-28**: Large file upload tested successfully - 10,026 rows Excel file read without errors. System ready for production use with files up to 160K rows.
**OPTIMIZATION 2025-08-28**: Added performance optimizations - database indexes, bulk import with COPY FROM, commit batching every 500 records, PostgreSQL optimizations. Two import options: regular (with progress visualization) and fast bulk import (5-10x faster).
**DEPLOYMENT 2025-08-28**: Created deployment guide and fix script for database migration issues. System ready for production deployment on Replit with PostgreSQL backend.
**RANKING MANAGEMENT 2025-08-29**: Implemented comprehensive ranking management system - users can view all created rankings, access detailed company lists within each ranking, and export rankings to PDF. Added navigation menu item "Управління рейтингами" for easy access. Fixed PDF export bugs by implementing dynamic latest ranking detection.
**USER MANAGEMENT & DATA INTEGRITY 2025-08-30**: Successfully resolved critical production-development database synchronization issue affecting 10,025 companies. Implemented complete user management interface for administrators with create/edit/delete functionality and role assignment (Administrator, Manager, Guest). Added automatic data correction system ensuring production data integrity. Established new workflow: developer makes changes → user verifies on https://rating-ua.replit.app/ → only after confirmation developer marks task complete. Fixed startup errors and KVED statistics display logic - system now stable with simplified statistics showing only company counts without misleading selection data when no selections exist. Improved KVED statistics table with "Актуалізовано" column showing actualized company counts and reorganized navigation menu with dropdown submenus for better usability (Завантаження/Файли, Рейтинг/Управління рейтингами).

# System Architecture

## Backend Framework
- **Flask**: Core web framework with modular blueprint structure
- **SQLAlchemy**: ORM for database operations with automatic table creation
- **Flask-Login**: Session management and user authentication

## Database Design
- **PostgreSQL**: Primary database (configurable via DATABASE_URL environment variable)
- **Models**: User, Company, Region, Kved, CompanySize, Financial, SelectionBase, SelectionCompany, Ranking, RankingCompany tables with proper relationships
- **Connection Pooling**: Configured with pool recycling and health checks
- **Sequential Processing**: Three-stage data processing with separate database storage for each stage

## Authentication & Authorization
- **Role-based Access Control**: Three user roles (admin, editor, viewer) with granular permissions
- **Session Management**: Flask-Login with remember-me functionality
- **Permission System**: Method-level permission checking for different operations

## Data Processing Pipeline
- **Excel/CSV Import**: pandas and openpyxl for file processing with progress tracking
- **Data Cleaning**: Numeric value normalization and validation for financial data
- **Duplicate Handling**: ЄДRPOU-based deduplication - updates existing companies, creates new ones
- **Modern SQLAlchemy**: Uses select() and execute() syntax for database operations
- **Three-Stage Architecture**:
  1. **Selection for Ranking**: Primary filtering (employees, revenue, profit) creates SelectionBase
  2. **Ranking Creation**: Secondary filters (KVED, region, size) applied to SelectionBase, creates Ranking
  3. **Export**: CSV and PDF export from completed rankings
- **Sequential Processing**: Each stage uses results from previous stage, not independent processing
- **Database Storage**: Separate tables for each stage - companies, selection_bases, rankings, financials
- **Dynamic Sorting**: Real-time ranking updates with ascending/descending order support

## File Management
- **Upload Handling**: Secure file upload with size limits (16MB) and type validation
- **File Processing**: Asynchronous processing of large datasets
- **Data Merging**: Capability to merge multiple files based on company identifiers

## Frontend Architecture
- **Template Engine**: Jinja2 with modular template inheritance
- **UI Framework**: Bootstrap 5 with dark theme and responsive design
- **Internationalization**: Ukrainian language interface
- **Progressive Enhancement**: JavaScript for enhanced user experience
- **Ranking Interface**: Reorganized form with compact KVED display (codes only), sorting functionality matching Companies page, real-time ranking updates

## API Design
- **RESTful Endpoints**: JSON API for company data with pagination
- **Query Parameters**: Flexible filtering and sorting options
- **Permission Integration**: API-level permission checking

## Security Features
- **Input Validation**: File type and size restrictions
- **SQL Injection Protection**: SQLAlchemy ORM prevents direct SQL injection
- **Session Security**: Secure session configuration with environment-based secrets
- **CSRF Protection**: Built-in Flask security features

# External Dependencies

## Core Framework Dependencies
- **Flask**: Web framework and routing
- **SQLAlchemy**: Database ORM and migrations
- **Flask-Login**: User session management
- **Werkzeug**: WSGI utilities and password hashing

## Data Processing Libraries
- **pandas**: Excel/CSV file processing and data manipulation
- **openpyxl**: Excel file reading and writing
- **numpy**: Numerical computations for data processing

## Database
- **PostgreSQL**: Primary database system
- **psycopg2**: PostgreSQL adapter for Python

## Frontend Assets
- **Bootstrap 5**: UI framework with Replit dark theme
- **Bootstrap Icons**: Icon library for UI elements

## Development & Deployment
- **ProxyFix**: WSGI middleware for proper header handling
- **Python logging**: Built-in logging system for debugging
- **Environment Variables**: Configuration management for database and secrets

## File System
- **Local File Storage**: Upload directory for temporary file processing
- **Static Assets**: CSS and JavaScript files served directly