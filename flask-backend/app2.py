# app.py
from flask import Flask, request, jsonify, send_file
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
import numpy as np
import traceback
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Подключение к PostgreSQL
DB_USER = 'postgres'
DB_PASSWORD = 'rjkz'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(DATABASE_URL)
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


@app.route('/api/upload', methods=['POST'])
def upload_data():
    data = request.json
    print('Получены данные:', data)
    try:
        df = pd.DataFrame([data])
        with engine.connect() as connection:
            df.to_sql('original_plan', con=connection, if_exists='append', index=False)
        return jsonify({"message": "Данные успешно загружены в базу данных."}), 200
    except Exception as e:
        print('Ошибка загрузки данных:', e)
        return jsonify({"message": f"Ошибка загрузки данных: {e}"}), 500


@app.route('/api/process', methods=['POST'])
def process_data():
    try:
        # 1) Оригинал из БД
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM original_plan", conn)

        # 2) Данные от фронта
        gn_data = request.json
        if not isinstance(gn_data, list) or not gn_data:
            return jsonify({"error": "Ожидается список записей для обработки"}), 400

        # Преобразуем типы полей в каждой записи списка
        for record in gn_data:
            try:
                # Приводим к нужным типам
                record['id_stroki_pp'] = int(record['id_stroki_pp'])
                record['plan_quantity'] = int(record['plan_quantity'])
                record['order_notnds_price'] = float(record['order_notnds_price'])
            except (ValueError, KeyError) as e:
                return jsonify({"error": f"Ошибка преобразования типов: {e}"}), 400

        gn = pd.DataFrame(gn_data)


        # 3) Проверка ID
        missing = []
        if "id_stroki_pp" not in df: missing.append("original_plan")
        if "id_stroki_pp" not in gn:  missing.append("gn")
        if missing:
            return jsonify({"error": f"id_stroki_pp не найден в: {', '.join(missing)}"}), 400

        # 4) Индекс
        df.set_index("id_stroki_pp", inplace=True)
        gn.set_index("id_stroki_pp", inplace=True)
        df1 = df.copy()

        # 5) Приведение типов
        dtypes = {
            'shippment_month': 'object', 'code_skmtr': 'Int64',
            'ei': 'object', 'supply_source': 'object',
            'plan_quantity': 'float64', 'order_notnds_price': 'float64'
        }
        for col, dtype in dtypes.items():
            if col in gn.columns:
                if dtype == 'object':
                    gn[col] = gn[col].replace('nan', np.nan).astype(str)
                else:
                    gn[col] = pd.to_numeric(gn[col], errors='coerce').astype(dtype)

        # 6) Обновляем df1
        for idx, row in gn.dropna(how="all").iterrows():
            for col, val in row.items():
                df1.at[idx, col] = val

        # 7) Merge по индексу
        result = pd.merge(
            df, df1,
            left_index=True, right_index=True,
            suffixes=("_PLAN","_FACT")
        )

        # 8) Выборка колонок с reindex
        columns_result = [
            "rd_name_PLAN", "supply_poligon_PLAN", "year_PLAN",
            "quarter_ship_PLAN", "shippment_month_PLAN", "delivery_month_PLAN",
            "code_statya_pb_PLAN", "name_statya_pb_PLAN", "code_skmtr_PLAN",
            "product_name_PLAN", "mark_che_PLAN", "gost_ost_tu_PLAN",
            "size_sort_PLAN", "supply_source_PLAN", "ei_PLAN",
            "plan_quantity_PLAN", "order_notnds_price_PLAN", "sum_fact_nds_PLAN",
            "shippment_month_FACT", "code_skmtr_FACT", "product_name_FACT",
            "mark_che_FACT", "gost_ost_tu_FACT", "size_sort_FACT",
            "supply_source_FACT", "plan_quantity_FACT",
            "order_notnds_price_FACT", "sum_fact_nds_FACT", "correction_type"
        ]
        result = result.reindex(columns=columns_result, fill_value=None)

        # 9) Добавляем недостающие
        for col in ["purchasing_group_name","sp_name","RKTSU","decision"]:
            if col not in result.columns:
                result[col] = None

        # 10) Пишем в БД
        with engine.begin() as conn:
            result.to_sql("result", conn, if_exists="replace", index=False)

        return jsonify({"message":"Обработка успешна"}), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/final', methods=['GET'])
def download_data():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(dir_path, 'Final_Result.xlsx')
    # Тут можно создать файл из таблицы result, если файла нет
    with engine.connect() as connection:
        result = pd.read_sql("SELECT * FROM result", connection)
    result.to_excel(file_path, index=False)
    return send_file(file_path, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
