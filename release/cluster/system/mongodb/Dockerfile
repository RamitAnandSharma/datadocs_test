FROM docker.io/postgres:9.4-alpine

MAINTAINER Tuan Nguyen <tuan08@gmail.com>

RUN apk update && \
    apk add bash git openssh rsync augeas && \
    deluser $(getent passwd 33 | cut -d: -f1) && \
    delgroup $(getent group 33 | cut -d: -f1) 2>/dev/null || true && \
    mkdir -p ~root/.ssh /etc/authorized_keys && chmod 700 ~root/.ssh/ && \
    augtool 'set /files/etc/ssh/sshd_config/AuthorizedKeysFile ".ssh/authorized_keys /etc/authorized_keys/%u"' && \
    echo -e "Port 22\n" >> /etc/ssh/sshd_config && \
    cp -a /etc/ssh /etc/ssh.cache && \
    rm -rf /var/cache/apk/*

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D", "-f", "/etc/ssh/sshd_config"]
