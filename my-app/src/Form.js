import React, { useState } from 'react';
import { uploadData } from './api';
import { Button } from './components/ui/Button';

const Form = ({ setFormData }) => {
  const [data, setData] = useState({});

  const handleChange = (e) => {
    const newData = { ...data, [e.target.name]: e.target.value };
    console.log('Текущее состояние формы:', newData); //  Добавил лог для отслеживания
    setData(newData);
    setFormData(newData);  // Обновляем formData в App при каждом изменении
  };

  const handleSubmit = async () => {
    console.log('Отправка данных:', data); //  Лог перед отправкой
    try {
      const response = await uploadData(data);
      console.log('Ответ от сервера:', response); //  Лог ответа
      alert('Данные успешно загружены в базу данных!');
    } catch (error) {
      alert('Ошибка при загрузке данных');
      console.error('Ошибка:', error);
    }
  };

  return (
    <div className="p-4 space-y-2">
      <input name="id_stroki_pp" placeholder="ID строки ПП" onChange={handleChange} />
      <input name="shippment_month" placeholder="Месяц отгрузки" onChange={handleChange} />
      <input name="code_skmtr" placeholder="Код СК-МТР" onChange={handleChange} />
      <input name="plan_quantity" placeholder="Количество" onChange={handleChange} />
      <input name="order_notnds_price" placeholder="Цена без НДС" onChange={handleChange} />
      <Button onClick={handleSubmit}>Загрузить в БД</Button>
    </div>
  );
};

export default Form;