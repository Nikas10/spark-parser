FROM gettyimages/spark:2.4.1-hadoop-3.0
MAINTAINER Nikita Abramenko

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre

RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get -y install ssh
RUN apt-get -y install wget
RUN apt-get -y install openssh-server
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN mv /usr/local/ssh_config $HOME/.ssh/config

COPY /config/hadoop/* $HADOOP_CONF_DIR/
COPY /config/ssh/ssh_config /usr/local/ssh_config
COPY shell/* /shell/
RUN chmod +x $HADOOP_CONF_DIR/hadoop-env.sh
RUN chmod +x /shell/yarn.sh
RUN chmod +x /shell/standalone.sh

RUN hdfs namenode -format

ENTRYPOINT ["/bin/bash", "-c", "service ssh start; tail -f /dev/null"]