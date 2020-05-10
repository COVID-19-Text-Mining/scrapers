FROM python:3.6-alpine3.7
COPY ./ /scraper

RUN apk update
RUN apk add libstdc++ libgcc libxslt libxml2 openssl libffi
# When building wheels, add this
#RUN apk add --no-cache --virtual .build-deps \
#   make automake gcc g++ python3-dev linux-headers \
#   libffi-dev openssl-dev libxml2-dev libxslt-dev
RUN pip install --upgrade pip

# Cleanup
#RUN apk del .build-deps
RUN rm -rf /root/.cache/pip \
    /scraper/wheels

ENV MONGO_HOSTNAME mongodb05.nersc.gov
ENV MONGO_DB COVID-19-text-mining
ENV MONGO_AUTHENTICATION_DB COVID-19-text-mining
# ENV MONGO_USERNAME ***
# ENV MONGO_PASSWORD ***
ENV PYTHONUNBUFFERED 1

ENTRYPOINT python /scraper/job.py
