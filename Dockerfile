FROM python:latest

COPY . /usr/app
WORKDIR /usr/app
RUN pip install -r requirements.txt

WORKDIR /usr/app/defichain_analyzer

CMD [ "python", "./main.py"]
