FROM postgres:14.4-bullseye AS riskbus_base
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y build-essential postgresql-server-dev-14
RUN apt-get install -y librdkafka-dev zlib1g-dev

COPY pg_kafka /opt/pg_kafka
RUN cd /opt/pg_kafka && mkdir --mode 777 stage dist && make && make install DESTDIR=/opt/pg_kafka/stage

RUN apt-get clean
ENTRYPOINT ["/bin/bash"]
