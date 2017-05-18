FROM python:2.7-alpine

RUN apk update && apk add git

RUN mkdir /app
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

LABEL maintainer="Kyle Lawlor <klawlor419@gmail.com>" \
      version="0.1"

CMD flask run --host=0.0.0.0 --port=5000
