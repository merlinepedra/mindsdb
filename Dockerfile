FROM ubuntu:20.04 as dev
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -y update && apt-get -y install curl git
RUN apt-get -y install python3.8 python3.8-venv python3-pip python3-wheel

RUN useradd --uid 5000 --create-home --shell /bin/bash -p '' dev
RUN adduser dev sudo

USER dev
WORKDIR /home/dev

# RUN git clone https://github.com/mindsdb/mindsdb.git
# WORKDIR /home/dev/mindsdb

# # COPY . .

# RUN python3 -m venv mindsdb
# RUN bash -c 'source mindsdb/bin/activate'

# COPY requirements.txt .
# RUN pip install -r requirements.txt

# COPY setup.py .
# RUN python3 setup.py develop

# 
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
