FROM frolvlad/alpine-java
# INSTALAR HADOOP 
VOLUME /tmp

ADD mlgia-dataset-service-0.0.1-SNAPSHOT.jar app.jar

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]