FROM python:3.6-alpine3.7
COPY ./ /scraper

RUN apk update
RUN apk add libstdc++ libgcc libxslt libxml2 openssl libffi
# When building wheels, add this
#RUN apk add --no-cache --virtual .build-deps \
#   make automake gcc g++ python3-dev linux-headers \
#   libffi-dev openssl-dev libxml2-dev libxslt-dev
RUN pip install --upgrade pip
RUN pip install  /scraper/wheels/bcrypt-3.1.7-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/cffi-1.14.0-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/cryptography-2.9.1-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/lxml-4.5.0-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/numpy-1.18.2-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/pandas-1.0.3-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/pdfminer-20191125-py3-none-any.whl\
    /scraper/wheels/Protego-0.1.16-py3-none-any.whl\
    /scraper/wheels/pycryptodome-3.9.7-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/PyDispatcher-2.0.5-py3-none-any.whl\
    /scraper/wheels/pymongo-3.10.1-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/PyNaCl-1.3.0-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/PyPDF2-1.26.0-py3-none-any.whl\
    /scraper/wheels/pysftp-0.2.9-py3-none-any.whl\
    /scraper/wheels/Twisted-20.3.0-cp36-cp36m-linux_x86_64.whl\
    /scraper/wheels/zope.interface-5.1.0-cp36-cp36m-linux_x86_64.whl
RUN pip install -r /scraper/requirements.txt

# Cleanup
#RUN apk del .build-deps
RUN rm -rf /root/.cache/pip \
    /scraper/wheels

ENV MONGO_HOSTNAME mongodb05.nersc.gov
ENV MONGO_DB COVID-19-text-mining
ENV MONGO_AUTHENTICATION_DB COVID-19-text-mining
ENV MONGO_USERNAME ***
ENV MONGO_PASSWORD ***
ENV PYTHONUNBUFFERED 1

ENTRYPOINT python /scraper/job.py
