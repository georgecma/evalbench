FROM ubuntu:jammy
ENV DEBIAN_FRONTEND=noninteractive
RUN \
  apt-get update -y && \
  apt-get install -y unzip  && \
  apt-get install -y git  && \
  apt-get install -y wget && \
  apt-get install -y apt-transport-https ca-certificates gnupg && \
  apt-get install -y curl && \
  apt-get install -y vim && \
  apt-get install -y jq && \
  # Move these dependencies to cloud run when enabled
  apt-get install -y maven && \ 
  apt-get install -y openjdk-17-jdk && \
  apt-get install -y make python3 python3-pip

RUN git clone https://gerrit.googlesource.com/gcompute-tools /root/gcompute-tools

WORKDIR /root

CMD ["/usr/bin/python3", "/root/gcompute-tools/git-cookie-authdaemon", "--nofork"]