import os
import sys

from config import settings

# Добавляем текущую директорию в путь, чтобы Python видел пакеты (core, config)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Импортируем models из __init__, чтобы подтянулись все классы (RawData, Report)
from core.models import Base, BomReport
from core.repositories.raw_repository import RawDataRepository
from core.services.material_service import MaterialETLService


def main():
    print("=== ЗАПУСК ПРОВЕРКИ ===")

    # 1. Настройка Базы Данных
    print(f"Подключение к БД: {settings.DB_URL}")
    engine = create_engine(settings.DB_URL, echo=False)  # echo=True для отладки SQL

    # Создаем таблицы (raw_factory_data и bom_reports)
    # Если таблицы уже есть, этот шаг их не перезапишет
    Base.metadata.create_all(engine)

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    try:
        # 2. Сборка зависимостей (DI)
        # Создаем репозиторий (отвечает за работу с БД)
        repository = RawDataRepository(session)
        # Создаем сервис (отвечает за бизнес-логику), передаем в него репозиторий
        etl_service = MaterialETLService(repository)

        # 3. Шаг ETL (Extract -> Transform -> Load)
        print("\n--- Шаг 1: Импорт данных ---")
        if os.path.exists(settings.INPUT_CSV_PATH):
            rows_inserted = etl_service.run_import_pipeline()
            print(f"--> Успешно загружено строк в сырую таблицу: {rows_inserted}")
        else:
            print(f"!!! Ошибка: Файл {settings.INPUT_CSV_PATH} не найден.")
            return

        # 4. Шаг SQL Calculation (BOM Explosion)
        print("\n--- Шаг 2: Разузлование (SQL Recursive CTE) ---")
        raw_report_data = etl_service.generate_bom_report()

        # 5. Вывод результатов
        print(f"--> Получено строк отчета: {len(raw_report_data)}")

        if not raw_report_data:
            print("!!! Отчет пуст. Проверьте, есть ли в данных записи с type='FIN'.")
            return

        print("\n--- Результат (Первые 10 строк) ---")
        # Форматирование заголовка таблицы
        header_fmt = "{:<8} | {:<4} | {:<12} | {:<12} | {:<12} | {:<8}"
        print(
            header_fmt.format("Plant", "Year", "FIN ID", "Prod ID", "Comp ID", "Type")
        )
        print("-" * 70)

        for i, row in enumerate(raw_report_data):
            if i >= 10:
                break  # Ограничим вывод

            # Маппинг: Превращаем сырую строку SQL в объект BomReport (опционально)
            # Это хорошая проверка, что наша модель Report.py совпадает с SQL
            report_item = BomReport(
                plant=row.plant,
                year=row.year,
                fin_material_id=row.fin_material_id,
                prod_material_id=row.prod_material_id,
                component_id=row.component_id,
                component_material_release_type=row.component_material_release_type,
            )

            print(
                header_fmt.format(
                    report_item.plant,
                    report_item.year,
                    report_item.fin_material_id,
                    report_item.prod_material_id,
                    report_item.component_id,
                    str(report_item.component_material_release_type or "-"),
                )
            )

        print(f"\n... всего {len(raw_report_data)} строк.")
        print("\n=== УСПЕШНО ЗАВЕРШЕНО ===")

    except Exception as e:
        print(f"\n!!! КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback

        traceback.print_exc()
    finally:
        session.close()


if __name__ == "__main__":
    main()
