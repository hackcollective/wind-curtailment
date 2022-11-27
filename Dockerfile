FROM python:3.10-slim as base

RUN apt-get update && apt-get install && \
    apt-get install g++ --yes && \
    apt-get install wkhtmltopdf --yes && \
    pip3 install argon2-cffi && \
    apt-get install libpq-dev --yes

COPY ./requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY ./lib /src/lib
COPY ./scripts /src/scripts
COPY ./data /src/data

COPY main.py /src/main.py

WORKDIR /src

ENV PORT=8082
EXPOSE $PORT

RUN export PYTHONPATH=${PYTHONPATH}:/src/lib

CMD streamlit run main.py --server.port $PORT  --logger.level=info