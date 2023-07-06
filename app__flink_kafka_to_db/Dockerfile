FROM rappdw/docker-java-python:zulu11.43-python3.7.9
WORKDIR /usr/app

RUN pip install --upgrade pip

RUN pip install "apache-flink>=1.13.0,<1.14"
COPY . /usr/app
RUN pip install -r requirements.txt

CMD ["python", "main.py"]