FROM ubuntu:jammy
ENV DEBIAN_FRONTEND=noninteractive
RUN \
  apt-get update -y && \
  apt-get install -y unzip  && \
  apt-get install -y git  && \
  apt-get install -y ssh  && \
  apt-get install -y gcc-multilib  && \
  apt-get install -y git  && \
  apt-get install -y wget && \
  apt-get install -y apt-transport-https ca-certificates gnupg && \
  apt-get install -y curl && \
  apt-get install -y vim && \
  apt-get install -y jq && \
  apt-get install -y procps && \
  apt-get install -y make python3 python3-pip

RUN git clone https://gerrit.googlesource.com/gcompute-tools/
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir grpcio-tools

COPY . evalbench
WORKDIR evalbench
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN make proto -f ./Makefile

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --force-reinstall googleapis-common-protos==1.64.0
CMD ["/evalbench/run_service.sh"]
EXPOSE 50051
