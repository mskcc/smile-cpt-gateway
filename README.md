# CMO MetaDB CPT Gateway 🔍

The CPT Gateway is a downstream subscriber to any new messages processed/persisted by CMO METADB.

## Run

### Custom properties

Make an `application.properties` based on [application.properties.EXAMPLE](src/main/resources/application.properties.EXAMPLE). 

All properties are required with the exception of some NATS connection-specific properties. The following are only required if `nats.tls_channel` is set to `true`:

- `nats.keystore_path` : path to client keystore
- `nats.truststore_path` : path to client truststore
- `nats.key_password` : keystore password
- `nats.store_password` : truststore password

### Locally

**Requirements:**
- maven 3.6.1
- java 8

Add `application.properties` and `log4j.properties` to the local application resources: `src/main/resources`

Build with 

```
mvn clean install
```

Run with 

```
java -jar server/target/cmo_metadb_cpt_gateway.jar
```

### With Docker

**Requirements**
- docker

Build image with Docker

```
docker build -t <repo>/<tag>:<version> .
```

Push image to DockerHub 

```
docker push <repo>/<tag>:<version>
```

If the Docker image is built with the properties baked in then simply run with:


```
docker run --name cpt-gateway <repo>/<tag>:<version> \
	-jar /cpt-gateway/cmo_metadb_cpt_gateway.jar
```

Otherwise use a bind mount to make the local files available to the Docker image and add  `--spring.config.location` to the java arg

```
docker run --mount type=bind,source=<local path to properties files>,target=/cpt-gateway/src/main/resources \
	--name cpt-gateway <repo>/<tag>:<version> \
	-jar /cpt-gateway/cmo_metadb_cpt_gateway.jar \
	--spring.config.location=/cpt-gateway/src/main/resources/application.properties
```
