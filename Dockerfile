FROM python:3.10-slim as base

RUN apt-get update && apt-get install && \
    apt-get install g++ --yes && \
    apt-get install wkhtmltopdf --yes && \
    pip3 install argon2-cffi && \
    apt-get install libpq-dev --yes \

RUN sudo apt-get install libxml2-dev libxslt-dev python-dev

COPY ./requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY ./lib /src/lib
COPY ./sql /src/sql
COPY ./scripts /src/scripts
COPY ./data /src/data
COPY ./tests /src/tests

COPY main.py /src/main.py
COPY etl.py /src/etl.py

WORKDIR /src

ENV PORT=8082
EXPOSE $PORT

RUN export PYTHONPATH=${PYTHONPATH}:/src/lib

FROM base as app

CMD streamlit run main.py --server.port $PORT  --logger.level=info

FROM base as etl
ENV PORT=8000
EXPOSE $PORT
CMD uvicorn etl:app --reload --host "0.0.0.0"
