import React, { useState } from 'react';
import Form from './Form';
import { processData, downloadFile } from './api';
import { Button } from './components/ui/Button';

const App = () => {
  const [formData, setFormData] = useState(null);

  const handleProcess = async () => {
    if (!formData) {
      alert('Данные для обработки не загружены!');
      return;
    }
    try {
      await processData(formData);
      alert('Данные успешно обработаны!');
    } catch (error) {
      console.error(error);
      alert('Ошибка при обработке данных');
    }
  };

  const handleDownload = async () => {
    try {
      await downloadFile();
      alert('Файл успешно скачан!');
    } catch (error) {
      console.error(error);
      alert('Ошибка при скачивании файла');
    }
  };

  return (
    <div className="p-4 space-y-4">
      <Form setFormData={setFormData} />
      <Button onClick={handleProcess} className="bg-blue-500 text-white">
        Обработать данные
      </Button>
      <Button onClick={handleDownload} className="bg-green-500 text-white">
        Скачать Excel
      </Button>
    </div>
  );
};

export default App;
