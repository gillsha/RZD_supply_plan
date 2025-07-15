import axios from 'axios';

const API_URL = 'http://localhost:5000/api';

export const uploadData = async (data) => {
  try {
    const response = await axios.post(`${API_URL}/upload`, data);
    return response.data;
  } catch (error) {
    console.error('Ошибка при загрузке данных:', error);
    throw error;
  }
};

export const processData = async (data) => {
  try {
    // Данные должны быть массивом объектов
    const response = await axios.post(`${API_URL}/process`, Array.isArray(data) ? data : [data]);
    return response.data;
  } catch (error) {
    console.error('Ошибка при обработке данных:', error);
    throw error;
  }
};

export const downloadFile = async () => {
  try {
    const response = await axios.get(`${API_URL}/final`, {
      responseType: 'blob',
    });
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', 'Final_Result.xlsx');
    document.body.appendChild(link);
    link.click();
  } catch (error) {
    console.error('Ошибка при загрузке файла:', error);
    throw error;
  }
};
