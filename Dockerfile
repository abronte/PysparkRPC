FROM python:3.7

RUN apt-get update && \
  apt-get install -y wget openjdk-11-jdk

RUN pip install pyspark==3.0.0 numpy pytest

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
RUN ln -s /root/.poetry/bin/poetry /usr/bin/poetry

ADD . /srv
WORKDIR /srv

RUN poetry export -f requirements.txt > requirements.txt && pip install -r requirements.txt

ENV PYTHONPATH /srv

CMD (nohup python pysparkapi/server.py > server.out &) && pytest
