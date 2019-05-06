FROM registry.centos.org/che-stacks/centos-stack-base

MAINTAINER Dharmit Shah <dshah@redhat.com>

EXPOSE 1337 3000 4200 5000 9000 8003
LABEL che:server:8003:ref=angular che:server:8003:protocol=http che:server:3000:ref=node-3000 che:server:3000:protocol=http che:server:9000:ref=node-9000 che:server:9000:protocol=http

RUN sudo yum update -y && \
    sudo yum -y install rh-nodejs8 && \
    sudo yum -y groupinstall 'Development Tools' && \
    sudo yum -y install libssl1.0-dev && \
    sudo yum -y install liblz4-dev && \
    sudo yum -y install libpthread-stubs0-dev && \
    sudo yum -y install libsasl2-dev && \
    sudo yum -y install libsasl2-modules && \
    sudo yum -y install make && \
    sudo yum -y install python && \
    sudo yum -y install nodejs npm ca-certificates && \
    sudo yum -y clean all && \
    sudo ln -s /opt/rh/rh-nodejs8/root/usr/bin/node /usr/local/bin/nodejs && \
    sudo scl enable rh-nodejs8 'npm install --unsafe-perm -g gulp bower grunt grunt-cli yeoman-generator yo generator-angular generator-karma generator-webapp' && \
    cat /opt/rh/rh-nodejs8/enable >> /home/user/.bashrc

ENV PATH=/opt/rh/rh-nodejs8/root/usr/bin${PATH:+:${PATH}}
ENV LD_LIBRARY_PATH=/opt/rh/rh-nodejs8/root/usr/lib64${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
ENV PYTHONPATH=/opt/rh/rh-nodejs8/root/usr/lib/python2.7/site-packages${PYTHONPATH:+:${PYTHONPATH}}
ENV MANPATH=/opt/rh/rh-nodejs8/root/usr/share/man:$MANPATH
