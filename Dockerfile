
FROM python:3.8-slim

WORKDIR /app

ADD . /app

RUN pip install -r requirements.txt

EXPOSE 9009

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9009"]