Spring REST
==================

The Gaffer Spring REST module is an implementation of the Gaffer REST API deployed 
in a Spring Boot container. It is relatively new compared to the Jersey / Jaxrs
REST war but should provide the following benefits:
* Easier to extend through spring boot plugins
* Easier to add dependencies at deployment time (no need to re-build a WAR file)
* Easier to deploy (you only need java)

It is still early in its development lifecycle so is still experimental at this stage and
is likely to contain bugs which will be fixed in subsequent releases and may be subject
to breaking changes.

However, going forward into Gaffer v2.0 we hope this to become the standard for how we
build and deploy REST APIs.

At the moment you might notice that the version is marked as v2.0-alpha. This represents
its maturity compared to that of the Jersey REST API. Once it can fulfil all the endpoints
the Jersey REST API can, we will change it to match the 2.0 version.

### Implemented Features
* Operations endpoint
* Graph configuration endpoint
* Properties endpoint
* Status endpoint

### Features that we have yet to implement
* Chunked endpoint

### Features we don't plan to implement
* Custom Swagger UI with operation chain builder
* Supporting older versions of the API


### How to run
With maven from the root of the project:
```bash
mvn spring-boot:run -pl :spring-rest -Pdemo
```

With java
```
java \
-Dgaffer.schemas=/path/to/schemas \
-Dgaffer.storeProperties=/path/to/store.properties \
-Dgaffer.graph.config=/path/to/graphConfig.json \
-Dloader.path=/path/to/external/libs \
-jar target/spring-rest-1.13.1-SNAPSHOT.jar 
```

You can alternatively add the gaffer system properties to your `application.properties` file

Once running, open the browser to http://localhost:8080/rest

You can change the context root by changing the `server.context-path` value in 
`application.properties` 