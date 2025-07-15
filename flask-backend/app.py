# app.py
from flask import Flask, request, jsonify, send_file
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
import numpy as np
import traceback

app = Flask(__name__)

# Подключение к PostgreSQL
DB_USER = 'postgres'
DB_PASSWORD = 'liza'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

@app.route('/api/upload', methods=['POST'])
def upload_data():
    data = request.json
    df = pd.DataFrame([data])
    try:
        with engine.connect() as connection:
            df.to_sql('original_plan', con=connection, if_exists='append', index=False)
        return jsonify({"message": "Данные успешно загружены в базу данных."}), 200
    except Exception as e:
        return jsonify({"message": f"Ошибка загрузки данных: {e}"}), 500

@app.route('/api/process', methods=['POST'])
def process_data():
    try:
        with engine.connect() as connection:
            # Чтение данных из базы
            df = pd.read_sql("SELECT * FROM original_plan", connection)

            # Забираем данные gn из POST-запроса
            gn_data = request.json
        
            if not gn_data:
                return jsonify({"error": "Данные gn_table не были переданы"}), 400
        
            # Преобразуем данные в DataFrame
            gn = pd.DataFrame(gn_data)
   
        # Проверяем наличие нужного индекса в обеих таблицах
        missing_columns = []
        if "id_stroki_pp" not in df.columns:
            missing_columns.append('original_plan')
        if "id_stroki_pp" not in gn.columns:
            missing_columns.append('gn_table')

        if missing_columns:
            raise KeyError(f"id_stroki_pp не найден в таблицах: {', '.join(missing_columns)}")

        # Установка индекса
        df.set_index("id_stroki_pp", inplace=True)
        gn.set_index("id_stroki_pp", inplace=True)

    except KeyError as ke:
        return jsonify({"error": str(ke)}), 400
    except Exception as e:
        return jsonify({"error": f"Ошибка при обработке данных: {e}"}), 500


        df1 = df.copy()

        dtypes_mapping = {
            'shippment_month': 'object',
            'code_skmtr': 'int64',
            'ei': 'object',
            'supply_source': 'object',
            'plan_quantity': 'float64',
            'order_notnds_price': 'float64',
            'note_for_supplier': 'object',
            'correction_type': 'object'
        }

        # Приведение типов в gn
        for column, dtype in dtypes_mapping.items():
            if column in gn.columns:
                if dtype == 'object':
                    # Безопасное преобразование в строку с заменой 'nan' на np.nan
                    gn[column] = gn[column].astype(str)
                    gn.loc[gn[column] == 'nan', column] = np.nan
                elif dtype == 'int64':
                    # Для целых чисел с поддержкой NaN
                    gn[column] = pd.to_numeric(gn[column], errors='coerce').astype('Int64')
                elif dtype == 'float64':
                    # Для дробных чисел
                    gn[column] = pd.to_numeric(gn[column], errors='coerce').astype('float64')
                    
        # Обновляем df1 значениями из gn (только не-NaN поля)
        for idx in gn.index:  # idx будет числовым (например, 2870034)
            if idx in df1.index:  # Проверяем наличие ID в df1
                # Берём только не-NaN значения из gn для этой строки
                non_nan_updates = gn.loc[idx].dropna()
                # Обновляем соответствующие столбцы в df1
                for col in non_nan_updates.index:
                    if col in df1.columns:
                        df1.at[idx, col] = non_nan_updates[col]

        # Создаём маску различий
        diff_mask = df != df1

        # Получаем различающиеся строки и столбцы
        differences = df1[diff_mask.any(axis=1)]

        # Выводим только изменённые значения (остальные будут NaN)
        differences = df1.where(diff_mask)
        differences = differences.dropna(how='all').dropna(axis=1, how='all')

        print("Изменённые строки:")
        print(differences)

        # Объединение таблиц 
        print("Начинаем объединение данных")

        # Словарь для преобразования названий столбцов в читаемые названия корректировок
        column_to_message = {
            'shippment_month': 'Изменен месяц на {}',
            'code_skmtr': 'Изменен Код СК-МТР на {}',
            'supply_source': 'Изменен Источник поставки на {}',
            'ei': 'Изменена ЕИ на {}',
            'plan_quantity': 'Изменено Количество на {}',
            'order_notnds_price': 'Изменена Цена без НДС на {}'
        }

        df1['correction_type'] = ''

        # Обходим все строки в таблице корректировок (gn)
        for idx in gn.index:
            if idx in df1.index:  # Проверяем, есть ли такой ID в df1
                # Получаем только не-NaN значения для этой строки
                updates = gn.loc[idx].dropna()
                
                # Собираем сообщения о корректировках
                correction_messages = []
                
                # Обновляем значения в df1 и формируем текст корректировки
                for col in updates.index:
                    if col in df1.columns:
                        new_value = updates[col]
                        df1.at[idx, col] = new_value
                        
                        # Формируем сообщение о корректировке
                        if col in column_to_message:
                            message_template = column_to_message[col]
                            correction_messages.append(message_template.format(new_value))
                
                # Если есть корректировки - объединяем их через запятую
                if correction_messages:
                    df1.at[idx, 'correction_type'] = ', '.join(correction_messages)
                    
        result = pd.merge(df, df1, left_index=True, right_index=True, suffixes=("_PLAN", "_FACT"))

        result = result.drop(['parent_id_pp_PLAN', 'order_month_PLAN', 'struk_podrazd_PLAN', 'sp_name_PLAN', 'code_rd_PLAN', 'filial_PLAN', 'service_code_PLAN', 'service_name_PLAN', 'order_skmtr_PLAN', 'okpd2_PLAN', 'ei_order_PLAN', 'classificator_code_PLAN', 'classificator_group_PLAN', 'activity_type_PLAN', 'price_status_PLAN', 'price_calc_tzr_PLAN', 'polzgroup_code_PLAN', 'polzgroup_name_PLAN', 'executor_PLAN', 'executor_name_PLAN', 'tu_PLAN', 'tu_name_PLAN', 'org_post_code_PLAN', 'org_post_name_PLAN', 'accum_centre_PLAN', 'accum_centre_name_PLAN', 'purchasing_group_PLAN', 'ofp_PLAN', 'consumer_structure_PLAN', 'rf_region_PLAN', 'terms_of_delivery_PLAN', 'goal_programm_PLAN', 'priority_PLAN', 'note1_PLAN', 'note2_PLAN', 'note3_PLAN', 'note4_PLAN', 'note5_PLAN', 'author_PLAN', 'change_sign_PLAN', 'delete_indicator_PLAN', 'planning_source_PLAN', 'currency_PLAN', 'lots_PLAN', 'pz_line_number_PLAN', 'procedure_protocol_PLAN', 'supplier_PLAN', 'msp_PLAN', 'produser_PLAN', 'purchase_agreement_PLAN', 'specification_PLAN', 'raznaryadka_PLAN', 'dzo_agreement_PLAN', 'dzo_specification_PLAN', 'order_quantity_PLAN', 'order_price_PLAN', 'nds_PLAN', 'price_withnds_PLAN', 'sum_notnds_PLAN', 'sum_nds_PLAN', 'withtzp_nds_price_PLAN', 'tzp_nds_price_PLAN', 'status_date_PLAN', 'creation_date_PLAN', 'pm_quantity_PLAN', 'raspred_quantity_PLAN', 'vidano_quantity_PLAN', 'nonds_sell_price_PLAN', 'nonds_realiz_price_PLAN', 'withnds_sell_price_PLAN', 'withnds_realization_price_PLAN', 'parent_id_pp_FACT', 'quarter_ship_FACT', 'order_month_FACT', 'struk_podrazd_FACT', 'sp_name_FACT', 'code_rd_FACT', 'filial_FACT', 'service_code_FACT', 'service_name_FACT', 'order_skmtr_FACT', 'okpd2_FACT', 'ei_order_FACT', 'classificator_code_FACT', 'classificator_group_FACT', 'activity_type_FACT', 'price_status_FACT', 'price_calc_tzr_FACT', 'polzgroup_code_FACT', 'polzgroup_name_FACT', 'executor_FACT', 'executor_name_FACT', 'tu_FACT', 'tu_name_FACT', 'org_post_code_FACT', 'org_post_name_FACT', 'accum_centre_FACT', 'accum_centre_name_FACT', 'purchasing_group_FACT', 'ofp_FACT', 'consumer_structure_FACT', 'rf_region_FACT', 'terms_of_delivery_FACT', 'goal_programm_FACT', 'priority_FACT', 'note1_FACT', 'note2_FACT', 'note3_FACT', 'note4_FACT', 'note5_FACT', 'author_FACT', 'change_sign_FACT', 'delete_indicator_FACT', 'planning_source_FACT', 'currency_FACT', 'lots_FACT', 'pz_line_number_FACT', 'procedure_protocol_FACT', 'supplier_FACT', 'msp_FACT', 'produser_FACT', 'purchase_agreement_FACT', 'specification_FACT', 'raznaryadka_FACT', 'dzo_agreement_FACT', 'dzo_specification_FACT', 'order_quantity_FACT', 'order_price_FACT', 'nds_FACT', 'price_withnds_FACT', 'sum_notnds_FACT', 'sum_nds_FACT', 'withtzp_nds_price_FACT', 'tzp_nds_price_FACT', 'status_date_FACT', 'creation_date_FACT', 'pm_quantity_FACT', 'raspred_quantity_FACT', 'vidano_quantity_FACT', 'nonds_sell_price_FACT', 'nonds_realiz_price_FACT', 'withnds_sell_price_FACT'], axis=1)

        columns_result = [
            "rd_name_PLAN",
            "supply_poligon_PLAN",
            "year_PLAN",
            "quarter_ship_PLAN",
            "shippment_month_PLAN",
            "delivery_month_PLAN",
            "code_statya_pb_PLAN",
            "name_statya_pb_PLAN",
            "code_skmtr_PLAN",
            "product_name_PLAN",
            "mark_che_PLAN",
            "gost_ost_tu_PLAN",
            "size_sort_PLAN",
            "supply_source_PLAN",
            "ei_PLAN",
            "plan_quantity_PLAN",
            "order_notnds_price_PLAN",
            "sum_fact_nds_PLAN",
            "shippment_month_FACT",
            "code_skmtr_FACT",
            "product_name_FACT",
            "mark_che_FACT",
            "gost_ost_tu_FACT",
            "size_sort_FACT",
            "supply_source_FACT",
            "plan_quantity_FACT",
            "order_notnds_price_FACT",
            "sum_fact_nds_FACT",
            "correction_type"  
        ]

        result = result[columns_result]

        new_columns = [
            "purchasing_group_name",
            "sp_name",
            "RKTSU",
            "decision"
        ]
        
        for col in new_columns:
            if col not in result.columns:
                result.loc[:, col] = None

#Грузим в SQL
        with engine.connect() as connection:
            result.to_sql('result', con=connection, if_exists='replace', index=False)

        return jsonify({"message": "Обработка успешна"})
    except Exception as e:
        traceback.print_exc()  # выведет ошибку в терминал
        return jsonify({"error": str(e)}), 500

@app.route('/api/final', methods=['GET'])
def download_data():
    file_path = os.path.join(os.getcwd(), 'Final_Result.xlsx')
    if not os.path.exists(file_path):
        # Тут можно создать файл из таблицы result, если файла нет
        with engine.connect() as connection:
            result = pd.read_sql("SELECT * FROM result", connection)
        result.to_excel(file_path, index=False)
    return send_file(file_path, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
