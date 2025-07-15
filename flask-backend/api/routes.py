from flask import Blueprint, request, jsonify, send_file
import os

api_bp = Blueprint('api', __name__)

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'data')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@api_bp.route('/api/upload', methods=['POST'])
def upload_data():
    data = request.json
    print("Данные загружены:", data)  # Просто вывод в консоль
    return jsonify({"message": "Данные успешно загружены!"})

@api_bp.route('/api/process', methods=['POST'])
def process_data():
    print("Запущена обработка данных")
    # Здесь будет твоя обработка
    return jsonify({"message": "Данные успешно обработаны!"})

@api_bp.route('/api/final', methods=['GET'])
def download_data():
    file_path = os.path.join(UPLOAD_FOLDER, 'Final_Result.xlsx')
    return send_file(file_path, as_attachment=True)