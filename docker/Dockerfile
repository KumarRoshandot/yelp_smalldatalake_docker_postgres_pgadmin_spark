FROM bitnami/spark

# Install prerequisites ( root user is used , as it was giving apt-get not found issue )
USER root
RUN apt-get update && apt-get install -y curl
USER 1001

RUN curl https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -o /opt/bitnami/spark/jars/postgresql-42.2.18.jar