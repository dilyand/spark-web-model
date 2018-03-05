FROM p7hb/docker-spark:2.1.0

# Prepare SBT and dependencies with temporary project snapshot
RUN mkdir /root/web-model
ADD . /root/web-model
RUN cd /root/web-model && sbt compile

# Mount actual project
VOLUME /root/web-model

