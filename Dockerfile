FROM ubuntu:20.04 as dev
ENV DEBIAN_FRONTEND noninteractive
SHELL ["/bin/bash", "-c"]
RUN apt-get -y update && apt-get -y install curl git
RUN apt-get -y install python3.8 python3.8-venv python3-pip

RUN useradd --uid 5000 --create-home --shell /bin/bash -p '' dev
RUN adduser dev sudo

USER dev
WORKDIR /home/dev

RUN git clone --branch lewis-dockerfile https://github.com/mindsdb/mindsdb.git
#RUN git clone https://github.com/mindsdb/mindsdb.git
WORKDIR /home/dev/mindsdb

RUN python3 -m venv mindsdb && source mindsdb/bin/activate && pip install wheel && pip install -r requirements.txt

RUN echo "python3 -m venv mindsdb" >> /home/dev/.bash_aliases
RUN echo "source mindsdb/bin/activate" >> /home/dev/.bash_aliases

#docker run \
#    --rm \
#    -it \
#    --cap-drop=all \
#    --security-opt no-new-privileges \
#    -u dev \
#    --cpus 4 \
#    --memory=1024m \
#    --memory-swap=1024m \
#    --memory-swappiness=0 \
#    --tmpfs /tmp:size=64m \
#    -v ${PWD}:/home/dev \
#    --security-opt seccomp=seccomp-perf.json \
#py-mindsdb

CMD [ "/bin/bash" ]
