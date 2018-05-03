FROM sickp/alpine-sshd:latest
RUN ssh-keygen -P "" -f /root/.ssh/id_rsa && \
        cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys && \
        passwd -d root
