FROM python:3.7-slim AS application

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip install -r requirements.txt

COPY . /usr/src/app

RUN pip install -e .

ENTRYPOINT ["python", "-m", "cdc"]

FROM application AS development

RUN pip install -r requirements-dev.txt
