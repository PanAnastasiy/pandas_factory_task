# 🏭 Factory BOM Explosion ETL

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?style=for-the-badge&logo=sqlalchemy&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=Docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)

Проект реализует **ETL-пайплайн** для обработки производственных данных и построения полного дерева состава изделия (**Bill of Materials - BOM Explosion**).

Архитектура построена на принципах **SOLID** и **Clean Architecture**, использует **SQLAlchemy** для работы с БД, **Pandas** для очистки данных и **Recursive CTE** (SQL) для расчета иерархий любой глубины.

---

## 🏗 Архитектура и Логика

Проект решает задачу разузлования (преобразование связей "Родитель-Компонент" в полное дерево) для производственных планов.

### 🔄 Data Flow (Поток данных)

1.  **Extract (Извлечение):**
    *   Чтение сырых CSV файлов с данными завода (`factory_data.csv`).
2.  **Transform (Трансформация - Python/Pandas):**
    *   Валидация структуры файлов.
    *   Очистка типов данных (удаление разделителей тысяч, обработка `NaN`).
    *   Дедупликация записей (схлопывание месячных планов в годовые уникальные связи).
    *   Стандартизация ID (добавление префиксов, приведение к строкам).
3.  **Load (Загрузка - Repository Pattern):**
    *   Массовая вставка (Bulk Insert) очищенных данных в PostgreSQL (`raw_factory_data`).
4.  **Calculation (SQL Logic):**
    *   Запуск **Recursive Common Table Expression (CTE)**.
    *   Построение иерархии от готового изделия (`FIN`) до сырья (`RM`/`ADD`) через все промежуточные переделы.
    *   Генерация плоского отчета для аналитики.

---

# 📂 Структура проекта

```text
pandas_factory_task/
├── .github/
│   └── workflows/
│       └── code-quality-and-tests.yml  # CI/CD pipelines for code quality and testing
│
├── core/                               # Core application logic
│   ├── models/                         # SQLAlchemy ORM models
│   ├── repositories/                   # Data access layer
│   ├── services/                       # Business logic layer
│   └── sql/                            # SQL scripts and procedures
│
├── docker_compose/                     # Docker orchestration
│   └── docker-compose.yml              # Multi-container setup
│
├── resources/                          # Project resources
│   ├── csv/                            # CSV data files
│   │   ├── processed/                  # Processed/transformed data
│   │   └── raw/                        # Raw input data
│   └──  mp3/                           # Audio resources (if applicable)
│   
├── tests/                              # Test files and data
│   
├── utils/                              # Configuration and utilities
│
├── main.ipynb                          # Main Jupyter notebook
└── Makefile                            # Command automation
```

# Командный справочник

Управление проектом осуществляется через `make`:

| Команда | Описание |
|---|---|
| **Инфструктура** | |
| `make up` | Поднять Docker контейнеры (Postgres) в фоне |
| `make down` | Остановить контейнеры |
| `make clean` | Остановить контейнеры и удалить данные (Volume) |
| `make logs` | Смотреть логи базы данных |
| `make postgres` | Зайти в SQL консоль (psql) внутри контейнера |
| **Разработка** | |
| `make format` | Автоматическое форматирование кода (black, isort) |
| `make lint` | Проверка стиля кода (flake8, black --check) |
| `make test` | Запуск модульных тестов (pytest) |
| **QA** | |
| `make check` | Полный цикл проверки: Format → Lint → Test |
| `make pre-commit-run` | Принудительный запуск всех пре-коммит хуков |
