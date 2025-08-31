# Керівництво з Deploy для Рейтинг.UA

## Проблема з міграціями
Якщо при deployment виникає помилка "Failed to validate database migrations - stage already exists":

### Рішення 1: Очистка бази даних
```bash
python fix_deployment.py
```

### Рішення 2: Ручне виправлення через Replit Database
1. Відкрийте вкладку "Database" в Replit
2. Видаліть всі тимчасові таблиці (temp_*)
3. Перезапустіть deployment

### Рішення 3: Новий deployment
1. Створіть новий deployment замість оновлення існуючого
2. В налаштуваннях оберіть "Create new deployment"
3. Налаштуйте нову базу даних

## Налаштування для Production

### Environment Variables (автоматично)
- `DATABASE_URL` - PostgreSQL connection string
- `SESSION_SECRET` - Flask session key
- `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` - PostgreSQL credentials

### Особливості deployment
- Система використовує PostgreSQL в production
- Всі індекси створюються автоматично
- Файли uploads/ зберігаються тимчасово
- Статичні файли обслуговуються через CDN

### Після успішного deployment
1. Перевірте доступність сайту за наданим URL
2. Протестуйте завантаження файлів
3. Перевірте роботу бази даних
4. Налаштуйте користувачів через админ панель

### Подальші оновлення
Після внесення змін в код:
1. Replit автоматично виявить зміни
2. Натисніть "Redeploy" в інтерфейсі deployment
3. Зміни застосуються без втрати даних

## Поширені проблеми

### "Stage already exists"
- Запустіть `python fix_deployment.py`
- Або створіть новий deployment

### Повільне завантаження файлів
- Використовуйте жовту кнопку "Швидко" (⚡) для великих файлів
- Індекси бази даних покращують швидкість

### Помилки з правами доступу
- Переконайтеся що користувач має роль 'admin' або 'editor'
- Перевірте налаштування в таблиці users