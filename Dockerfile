FROM maven:3.6.1-jdk-11-slim
# create working directory and set
RUN mkdir /cpt-gateway
ADD . /cpt-gateway
WORKDIR /cpt-gateway
RUN mvn clean install

# copy jar and set entrypoint
FROM openjdk:11-slim
COPY --from=0 /cpt-gateway/server/target/smile_cpt_gateway.jar /cpt-gateway/smile_cpt_gateway.jar
ENTRYPOINT ["java"]
