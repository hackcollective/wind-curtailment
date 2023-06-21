FROM python:3.10-slim as base

RUN apt-get update && apt-get install && \
    apt-get install g++ --yes && \
    apt-get install wkhtmltopdf --yes && \
    pip3 install argon2-cffi && \
    apt-get install libpq-dev --yes

RUN apt-get install libxml2-dev libxslt-dev --yes

COPY ./requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY ./static/index.html /usr/local/lib/python3.10/site-packages/streamlit/static/index.html


COPY ./lib /src/lib
COPY ./sql /src/sql
COPY ./scripts /src/scripts
COPY ./data /src/data
COPY ./tests /src/tests

COPY .streamlit /src/.streamlit
COPY main.py /src/main.py
COPY etl.py /src/etl.py

WORKDIR /src

ENV PORT=8082
EXPOSE $PORT

RUN export PYTHONPATH=${PYTHONPATH}:/src/lib
RUN export PYTHONPATH=${PYTHONPATH}:/lib

# make test app
FROM base as test
ENV PYTHONPATH=${PYTHONPATH}:/lib
CMD pytest

# make streamlit app
FROM base as app

CMD streamlit run main.py --server.port $PORT  --logger.level=info

# make etl runner
FROM base as etl
ENV PORT=8000
EXPOSE $PORT
CMD uvicorn etl:app --reload --host "0.0.0.0" --workers 4
