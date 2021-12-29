FROM python:latest

COPY . /usr/app
WORKDIR /usr/app
RUN pip install -r requirements.txt

WORKDIR /usr/app/src

CMD [ "python", "./defichain.py"]
