import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from config_nefatov import db_details


# Базовый URL нового эндпоинта Bitrix
BITRIX_REST_URL = "https://bitrix.vink.ru/rest/4079/5jnr93muyh8bn6dm/crm.company.list.json"


def db_connection():
    """Создает соединение с PostgreSQL"""
    return psycopg2.connect(
        dbname=db_details["dbname"],
        user=db_details["user"],
        password=db_details["password"],
        host=db_details["host"]
    )


def create_counteragents_table(conn):
    """Создает (если нет) таблицу и добавляет JSONB-колонки для телефонов и e-mail."""
    with conn.cursor() as cur:
        # Базовая таблица (как в старом скрипте), если её ещё нет
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dl_main_data.counteragents_bitrix (
                id VARCHAR(255) PRIMARY KEY,
                partner_code TEXT NOT NULL,
                company_type VARCHAR(50),
                title TEXT,
                lead_id VARCHAR(255),
                has_phone VARCHAR(1),
                has_email VARCHAR(1),
                has_imol VARCHAR(1),
                assigned_by_id VARCHAR(255),
                created_by_id VARCHAR(255),
                modify_by_id VARCHAR(255),
                banking_details TEXT,
                industry VARCHAR(50),
                revenue NUMERIC(20,4),
                currency_id VARCHAR(10),
                employees VARCHAR(50),
                comments TEXT,
                date_create TIMESTAMPTZ,
                date_modify TIMESTAMPTZ,
                opened VARCHAR(1),
                is_my_company VARCHAR(1),
                originator_id VARCHAR(255),
                origin_id VARCHAR(255),
                origin_version VARCHAR(255),
                last_activity_time TIMESTAMPTZ,
                address TEXT,
                address_2 TEXT,
                address_city TEXT,
                address_postal_code TEXT,
                address_region TEXT,
                address_province TEXT,
                address_country TEXT,
                address_country_code TEXT,
                address_loc_addr_id VARCHAR(255),
                address_legal TEXT,
                reg_address TEXT,
                reg_address_2 TEXT,
                reg_address_city TEXT,
                reg_address_postal_code TEXT,
                reg_address_region TEXT,
                reg_address_province TEXT,
                reg_address_country TEXT,
                reg_address_country_code TEXT,
                reg_address_loc_addr_id VARCHAR(255),
                utm_source TEXT,
                utm_medium TEXT,
                utm_campaign TEXT,
                utm_content TEXT,
                utm_term TEXT,
                parent_id_1050 VARCHAR(255),
                parent_id_1054 VARCHAR(255),
                parent_id_1066 VARCHAR(255),
                last_activity_by VARCHAR(255),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

        # Новые колонки под множественные телефоны/почты
        cur.execute(
            """
            ALTER TABLE dl_main_data.counteragents_bitrix
            ADD COLUMN IF NOT EXISTS phones JSONB;
            """
        )
        cur.execute(
            """
            ALTER TABLE dl_main_data.counteragents_bitrix
            ADD COLUMN IF NOT EXISTS emails JSONB;
            """
        )

        # Индекс по partner_code на всякий случай
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS counteragents_bitrix_partner_code_idx
            ON dl_main_data.counteragents_bitrix (partner_code);
            """
        )

        conn.commit()


def get_new_partner_codes(conn):
    """Получает новые коды партнеров, которых еще нет в Bitrix таблице"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT c."Партнер_Код"
            FROM dl_main_data.counteragents c
            LEFT JOIN dl_main_data.counteragents_bitrix cb 
                ON c."Партнер_Код" = cb.partner_code
            WHERE 
                c."Партнер_Код" IS NOT NULL 
                AND c."Партнер_Код" <> ''
                AND cb.partner_code IS NULL
            """
        )
        return [row[0] for row in cur.fetchall()]


def fetch_company_data(partner_code: str):
    """Получает из Bitrix только поля: ID, TITLE, PHONE, EMAIL по коду партнера"""
    params = {
        'filter[UF_CRM_COMPANY_CODE]': partner_code,
        'select[]': ['ID', 'TITLE', 'PHONE', 'EMAIL']
    }

    try:
        response = requests.get(BITRIX_REST_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'error' in data:
            print(f"Ошибка API для кода {partner_code}: {data['error']}")
            return None

        return data.get('result', [])
    except Exception as e:
        print(f"Ошибка при загрузке данных для кода {partner_code}: {str(e)}")
        return None


def _extract_values(multivalue_list):
    """Преобразует массив объектов Bitrix в список уникальных строковых VALUE."""
    if not isinstance(multivalue_list, list):
        return []
    seen = set()
    values = []
    for item in multivalue_list:
        value = (item or {}).get('VALUE')
        if not value:
            continue
        normalized = str(value).strip()
        if normalized not in seen:
            seen.add(normalized)
            values.append(normalized)
    return values


def transform_company_data(company: dict, partner_code: str):
    """Преобразует данные компании в словарь для UPSERT."""
    company_id = (company or {}).get('ID')
    title = (company or {}).get('TITLE')
    phones = _extract_values((company or {}).get('PHONE'))
    emails = _extract_values((company or {}).get('EMAIL'))

    return {
        'id': company_id,
        'partner_code': partner_code,
        'title': title,
        'phones': phones,
        'emails': emails,
        'updated_at': datetime.utcnow()
    }


def save_company_data(conn, company_data: dict):
    """UPSERT по id в dl_main_data.counteragents_bitrix для подмножества колонок."""
    if not company_data:
        return

    # Используем только перечисленные ключи; остальные поля остаются NULL/нетронутыми
    columns = ['id', 'partner_code', 'title', 'phones', 'emails', 'updated_at']
    values = [
        company_data.get('id'),
        company_data.get('partner_code'),
        company_data.get('title'),
        Json(company_data.get('phones', [])),
        Json(company_data.get('emails', [])),
        company_data.get('updated_at'),
    ]

    placeholders = ', '.join(['%s'] * len(columns))
    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])

    query = f"""
        INSERT INTO dl_main_data.counteragents_bitrix ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {updates}
    """

    with conn.cursor() as cur:
        cur.execute(query, values)
        conn.commit()


def main():
    """Основная функция для загрузки данных"""
    conn = db_connection()

    try:
        create_counteragents_table(conn)

        new_codes = get_new_partner_codes(conn)
        if not new_codes:
            print("Нет новых кодов для обработки")
            return

        print(f"Найдено новых кодов партнеров для обработки: {len(new_codes)}")

        for i, code in enumerate(new_codes, 1):
            print(f"Обработка кода [{i}/{len(new_codes)}]: {code}")

            companies = fetch_company_data(code)
            if not companies:
                print(f"  Не найдено данных для кода: {code}")
                # Помечаем код как обработанный (создаём запись-заглушку)
                placeholder = {
                    'id': f"EMPTY_{code}",
                    'partner_code': code,
                    'title': None,
                    'phones': [],
                    'emails': [],
                    'updated_at': datetime.utcnow()
                }
                save_company_data(conn, placeholder)
                print(f"  Создана пустая запись для кода: {code}")
                continue

            for company in companies:
                company_data = transform_company_data(company, code)
                save_company_data(conn, company_data)
                print(f"  Сохранена компания: {company.get('ID')} - {company.get('TITLE')}")

    except Exception as e:
        print(f"Критическая ошибка: {str(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

