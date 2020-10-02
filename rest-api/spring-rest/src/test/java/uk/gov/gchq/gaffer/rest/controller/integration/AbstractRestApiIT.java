/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller.integration;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;

import uk.gov.gchq.gaffer.rest.Application;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;

/**
 * Base class for Integration Tests.
 *
 * The AbstractRestApiIT starts the application with spring boot and provides {@code get()} and {@code post()} methods
 * for easy access, as well as a {@code checkResponse()} method which asserts that the correct status code is returned
 * and that the Gaffer Media type header was added.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(Application.class)
@WebIntegrationTest(randomPort = true)
public abstract class AbstractRestApiIT {
    private RestTemplate restTemplate = new TestRestTemplate();

    @Value("${local.server.port}")
    private int port;

    @Value("${server.context-path}")
    private String contextPath;

    protected String getBaseURl() {
        return "http://localhost:" + port + "/" + contextPath;
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

    protected <T> ResponseEntity<T> post(final String path, final String body, final Class<T> responseBodyClass) {
        try {
            return restTemplate.postForEntity(new URI(getBaseURl() + path), body, responseBodyClass);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Unable to constuct URI from " + getBaseURl() + path, e);
        }
    }

    protected void checkResponse(final ResponseEntity<?> response, final int expectedCode) {
        assertEquals(expectedCode, response.getStatusCode().value());
        assertTrue("Gaffer header was not present", response.getHeaders().containsKey(GAFFER_MEDIA_TYPE_HEADER));
    }
}
