FROM python:3.8
WORKDIR /app
ADD python1.py /app
ADD requirements.txt /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
CMD ["python", "./python1.py"]