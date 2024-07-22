FROM apache/airflow:2.9.3

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ENV OPENWEATHER_API_KEY=3a37f2c742c86e3a3a2695c0fb7c0b1d
