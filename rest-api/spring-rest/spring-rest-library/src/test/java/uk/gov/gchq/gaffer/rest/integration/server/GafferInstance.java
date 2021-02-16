/*
 * Copyright 2021 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.rest.integration.server;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class GafferInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferInstance.class);
    private static final FileFilter EXECUTABLE_FILTER = (file) -> file.getName().startsWith("spring-rest-implementation") && file.getName().endsWith("-exec.jar");
    private static final String REST_CONTEXT = "rest";
    private static final String ACTUATOR = "actuator";
    private static final long CHECK_INITIAL_WAIT_MS = TimeUnit.SECONDS.toMillis(5L);
    private static final long CHECK_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1L);

    private final RestTemplate template = new RestTemplate();
    private final String executeGraphOperation;
    private final List<String> arguments = new ArrayList<>();
    private final StartUpMonitor startUpMonitor;
    private final ShutdownMonitor shutdownMonitor;
    private final long maxStartupChecks = 30L;
    private final long maxShutdownChecks = 10L;

    private Status status = Status.IDLE;

    enum Status {
        IDLE, STARTING, STARTED, STOPPING, STOPPED;
    }

    public GafferInstance(final String executablePath,
                          final int port,
                          final String graphId,
                          final Path storeProperties,
                          final Path schema) {
        final String restUrl = ("http://localhost:" + port).concat("/".concat(REST_CONTEXT));
        this.executeGraphOperation = restUrl.concat("/graph/operations/execute");

        arguments.add("java");
        arguments.add("-jar");
        arguments.add(executablePath);

        /* Gaffer instance arguments */
        arguments.add("--server.port=" + port);
        arguments.add("--gaffer.graph.id=".concat(graphId));
        arguments.add("--gaffer.storeProperties=".concat(storeProperties.toString()));
        arguments.add("--gaffer.schemas=".concat(schema.toString()));

        /* Enable health checking and shutdown actuator operations over http */
        arguments.add("--management.endpoint.shutdown.enabled=true");
        arguments.add("--management.endpoints.web.exposure.include=health,shutdown");

        final String actuatorUrl = restUrl.concat("/".concat(ACTUATOR));
        startUpMonitor = new StartUpMonitor(actuatorUrl.concat("/health"));
        shutdownMonitor = new ShutdownMonitor(actuatorUrl.concat("/shutdown"));
    }

    public static String getPathToExecutable(final Class<?> clazz, final String modulePath) throws IllegalStateException {
        final Pattern projectHomePattern = Pattern.compile("^(.*?)".concat(modulePath.concat(".*?$")));
        final String currentPath = clazz.getResource(".").getPath();
        final Matcher matcher = projectHomePattern.matcher(currentPath);
        if (matcher.matches()) {
            final File file = new File(matcher.group(1).concat("/rest-api/spring-rest/spring-rest-implementation/target"));
            final File[] matchingFiles = file.listFiles(EXECUTABLE_FILTER);
            if (matchingFiles.length != 1) {
                throw new IllegalStateException(format("More than one matching executable found in directory: %s", file.getPath()));
            }
            return matchingFiles[0].getPath();
        }
        throw new IllegalStateException(format("Unable to establish current path: %s", currentPath));
    }

    public void start() {
        if (status != Status.IDLE) {
            LOGGER.info("Unable to start GafferInstance while Status is {}", status.name());
            return;
        }
        status = Status.STARTING;
        startServer();
        try {
            waitUntil(maxStartupChecks, startUpMonitor);
            if (status == Status.STARTING) {
                throw new IllegalStateException("Could not start GafferInstance");
            }
        } catch (final Exception exception) {
            throw new IllegalStateException("Failed to start GafferInstance", exception);
        }
        LOGGER.info("GafferInstance is running");
    }

    public void shutdown() {
        if (status != Status.STARTED) {
            LOGGER.error("Unable to shutdown GafferInstance while Status is {}", status.name());
        }
        status = Status.STOPPING;
        try {
            waitUntil(maxShutdownChecks, shutdownMonitor);
            if (status == Status.STOPPING) {
                throw new IllegalStateException("Could not shutdown GafferInstance");
            }
        } catch (final Exception exception) {
            throw new IllegalStateException("Failed to shutdown GafferInstance", exception);
        }
        LOGGER.info("GafferInstance is shutdown");
    }

    private void waitUntil(long maxChecks, Callable<Boolean> condition) throws Exception {
        Thread.sleep(CHECK_INITIAL_WAIT_MS);
        int checkCount = 0;
        while (checkCount < maxChecks) {
            if (condition.call()) {
                break;
            }
            Thread.sleep(CHECK_INTERVAL_MS);
            checkCount++;
        }
    }

    private void startServer() {
        final ProcessBuilder processBuilder = new ProcessBuilder(arguments);
        try {
            final Process process = processBuilder.start();
            ProcessMonitor.monitorInputStream(process);
            ProcessMonitor.monitorErrorStream(process);
            if (!process.isAlive()) {
                throwCouldNotStartProcessException();
            }
        } catch (IOException e) {
            throwCouldNotStartProcessException();
        }
    }

    private String throwCouldNotStartProcessException() {
        throw new IllegalStateException(format("Could not start process using arguments: %s", arguments));
    }


    private class StartUpMonitor implements Callable<Boolean> {
        private final String actuatorHealth;

        StartUpMonitor(final String actuatorHealth) {
            this.actuatorHealth = actuatorHealth;
        }

        public Boolean call()  {
            try {
                boolean isStarted = isSuccessful(get(actuatorHealth, JSONObject.class));
                if (isStarted) {
                    status = Status.STARTED;
                }
                return isStarted;
            } catch (final Exception e) {
                return false;
            }
        }
    }

    private class ShutdownMonitor implements Callable<Boolean> {
        private final String actuatorShutdown;

        ShutdownMonitor(final String actuatorShutdown) {
            this.actuatorShutdown = actuatorShutdown;
        }

        public Boolean call() {
            try {
                boolean isStopped = isSuccessful(post(actuatorShutdown, null, jsonContentTypeHttpHeaders(), JSONObject.class));
                if (isStopped) {
                    status = Status.STOPPED;
                }
                return isStopped;
            } catch (final Exception e) {
                return false;
            }
        }
    }

    private <T> boolean isSuccessful(final ResponseEntity<T> response) {
        return response != null && response.getStatusCode().is2xxSuccessful();
    }

    public <T> ResponseEntity<T> executeOperation(final Operation operation, final Class<T> clazz) throws SerialisationException {
        return post(executeGraphOperation, JSONSerialiser.serialise(operation), jsonContentTypeHttpHeaders(), clazz);
    }

    public <T> ResponseEntity<T> post(final String path, final Object body, final HttpHeaders headers, final Class<T> clazz) {
        final HttpEntity<Object> entity = new HttpEntity<>(body, headers);
        return template.postForEntity(path, entity, clazz);
    }

    public <T> ResponseEntity<T> get(final String path, final Class<T> clazz) {
        return template.getForEntity(path, clazz);
    }

    private HttpHeaders jsonContentTypeHttpHeaders() {
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }
}
