FROM python:3
WORKDIR /producer
COPY producer.py /producer/.
RUN apt-get update
RUN apt install python3-pip -y
RUN cd /producer
RUN git clone https://github.com/edenhill/librdkafka.git
RUN cd librdkafka/ && ./configure && make && make install && ldconfig
RUN pip3 install confluent-kafka Faker
CMD ["python3", "producer.py", "<producer-ip>:9092", "streamdata", "10000"]

