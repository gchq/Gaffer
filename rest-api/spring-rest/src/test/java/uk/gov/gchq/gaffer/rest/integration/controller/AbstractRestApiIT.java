/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.integration.controller;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.RestTemplate;

import uk.gov.gchq.gaffer.rest.GafferWebApplication;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;

/**
 * Base class for Integration Tests.
 *
 * The AbstractRestApiIT starts the application with spring boot and provides {@code get()} and {@code post()} methods
 * for easy access, as well as a {@code checkResponse()} method which asserts that the correct status code is returned
 * and that the Gaffer Media type header was added.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = GafferWebApplication.class, webEnvironment = RANDOM_PORT)
@EnableConfigurationProperties(value = AbstractRestApiIT.TestConfiguration.class)
@ActiveProfiles("test")
public abstract class AbstractRestApiIT {
    @Autowired
    private RestTemplate restTemplate;

    @Value("${local.server.port}")
    private int port;

    @Value("${server.servlet.context-path}")
    private String contextPath;

    @AfterAll
    public static void clearSystemProperties() {
        Properties properties = System.getProperties();
        List<String> propertiesToBeRemoved = new ArrayList<>();
        properties.forEach((key, value) -> {
            if (key instanceof String && ((String) key).startsWith("gaffer")) {
                propertiesToBeRemoved.add((String) key);
            }
        });

        propertiesToBeRemoved.forEach(System::clearProperty);
    }

    protected int getPort() {
        return port;
    }

    protected String getContextPath() {
        // All the rest endpoints are under /rest context
        return contextPath + "rest";
    }

    protected String getBaseURl() {
        return "http://localhost:" + port + "/" + getContextPath();
    }

    protected <T> ResponseEntity<T> get(final String path, final Class<T> responseBodyClass) {
        try {
            return restTemplate.getForEntity(new URI(getBaseURl() + path), responseBodyClass);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Unable to constuct URI from " + getBaseURl() + path, e);
        }
    }

    protected <T> ResponseEntity<T> request(final String path, final HttpMethod method, final HttpEntity entity, final Class<T> responseBodyClass) {
        try {
            return restTemplate.exchange(new URI(getBaseURl() + path), method, entity, responseBodyClass);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Unable to constuct URI from " + getBaseURl() + path, e);
        }
    }

    protected <T> ResponseEntity<T> post(final String path, final Object body, final Class<T> responseBodyClass) {
        try {
            return restTemplate.postForEntity(new URI(getBaseURl() + path), body, responseBodyClass);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Unable to constuct URI from " + getBaseURl() + path, e);
        }
    }

    protected void checkResponse(final ResponseEntity<?> response, final int expectedCode) {
        assertThat(response.getStatusCode().value()).isEqualTo(expectedCode);
        assertThat(response.getHeaders()).as("Gaffer header was not present").containsKey(GAFFER_MEDIA_TYPE_HEADER);
    }

    @Configuration
    @ConfigurationProperties(prefix = "server")
    public static class TestConfiguration {
        private final String contextRoot = "";
        private final int port = 0;

        public String getContextRoot() {
            return contextRoot;
        }

        public int getPort() {
            return port;
        }
    }
}
