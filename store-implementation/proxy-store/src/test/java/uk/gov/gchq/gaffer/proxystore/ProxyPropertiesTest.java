/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.proxystore;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.CONNECT_TIMEOUT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.DEFAULT_CONNECT_TIMEOUT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.DEFAULT_GAFFER_CONTEXT_ROOT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.DEFAULT_GAFFER_PORT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.DEFAULT_READ_TIMEOUT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_PORT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.READ_TIMEOUT;

public class ProxyPropertiesTest {
    @Test
    public void shouldLoadProxyPropertiesFromFileWithConstructor() {
        // Given
        Path propFilePath = Paths.get(ProxyStore.class.getResource("/proxy-store.properties").getFile());

        // When
        ProxyProperties proxy = new ProxyProperties(propFilePath);

        // Then
        assertEquals("/rest/v2", proxy.getGafferContextRoot());
    }

    @Test
    public void shouldLoadProxyPropertiesFromFileWithMethodString() {
        // Given
        String propFileStr = ProxyStore.class.getResource("/proxy-store.properties").getFile();

        // When
        ProxyProperties proxy = ProxyProperties.loadStoreProperties(propFileStr);

        // Then
        assertEquals("/rest/v2", proxy.getGafferContextRoot());
    }

    @Test
    public void shouldLoadProxyPropertiesFromFileWithMethodPath() {
        // Given
        Path propFilePath = Paths.get(ProxyStore.class.getResource("/proxy-store.properties").getFile());

        // When
        ProxyProperties proxy = ProxyProperties.loadStoreProperties(propFilePath);

        // Then
        assertEquals("/rest/v2", proxy.getGafferContextRoot());
    }

    @Test
    public void shouldSetAndGetConnectTimeout() {
        // Given
        ProxyProperties defaultTimeoutProxy = new ProxyProperties();
        ProxyProperties validTimeoutProxy = new ProxyProperties();
        ProxyProperties invalidTimeoutProxy = new ProxyProperties();

        // When
        validTimeoutProxy.setConnectTimeout(30);
        invalidTimeoutProxy.set(CONNECT_TIMEOUT, "30 secs");

        // Then
        assertEquals(DEFAULT_CONNECT_TIMEOUT, defaultTimeoutProxy.getConnectTimeout());
        assertEquals(30, validTimeoutProxy.getConnectTimeout());
        Exception e = assertThrows(IllegalArgumentException.class, invalidTimeoutProxy::getConnectTimeout);
        assertEquals("Unable to convert gaffer timeout into an integer", e.getMessage());
    }

    @Test
    public void shouldSetAndGetReadTimeout() {
        // Given
        ProxyProperties defaultTimeoutProxy = new ProxyProperties();
        ProxyProperties validTimeoutProxy = new ProxyProperties();
        ProxyProperties invalidTimeoutProxy = new ProxyProperties();

        // When
        validTimeoutProxy.setReadTimeout(30);
        invalidTimeoutProxy.set(READ_TIMEOUT, "30 secs");

        // Then
        assertEquals(DEFAULT_READ_TIMEOUT, defaultTimeoutProxy.getReadTimeout());
        assertEquals(30, validTimeoutProxy.getReadTimeout());
        Exception e = assertThrows(IllegalArgumentException.class, invalidTimeoutProxy::getReadTimeout);
        assertEquals("Unable to convert gaffer timeout into an integer", e.getMessage());
    }

    @Test
    public void shouldSetAndGetPort() {
        // Given
        ProxyProperties defaultPortProxy = new ProxyProperties();
        ProxyProperties validPortProxy = new ProxyProperties();
        ProxyProperties invalidPortProxy = new ProxyProperties();

        // When
        validPortProxy.setGafferPort(8443);
        invalidPortProxy.set(GAFFER_PORT, "8 443");

        // Then
        assertEquals(DEFAULT_GAFFER_PORT, defaultPortProxy.getGafferPort());
        assertEquals(8443, validPortProxy.getGafferPort());
        Exception e = assertThrows(IllegalArgumentException.class, invalidPortProxy::getGafferPort);
        assertEquals("Unable to convert gaffer port into an integer", e.getMessage());
    }

    @Test
    public void shouldGetURLWithSuffix() {
        // Given
        ProxyProperties proxy = new ProxyProperties();

        // When
        URL withEmptySuffix = proxy.getGafferUrl("");
        URL withValidSuffix = proxy.getGafferUrl("/content");
        URL withMissingSlashSuffix = proxy.getGafferUrl("path/content");

        // Then
        assertEquals("http://localhost:8080/rest", withEmptySuffix.toExternalForm());
        assertEquals("http://localhost:8080/rest/content", withValidSuffix.toExternalForm());
        assertEquals("http://localhost:8080/rest/path/content", withMissingSlashSuffix.toExternalForm());
    }

    @Test
    public void shouldThrowGetURLWithInvalidProtocol() {
        // Given
        ProxyProperties proxy = new ProxyProperties();

        // When / Then
        Exception e = assertThrows(IllegalArgumentException.class, () -> proxy.getGafferUrl("gaffer", "/content"));
        assertEquals("Could not create Gaffer URL from host (localhost), port (8080) and context root (/rest)", e.getMessage());
    }

    @Test
    public void shouldHandleDifferentContextRootValuesAndGetURLWithSuffix() {
        // Given
        ProxyProperties defaultContextRoot = new ProxyProperties();
        ProxyProperties emptyContextRoot = new ProxyProperties();
        ProxyProperties slashContextRoot = new ProxyProperties();
        ProxyProperties normalContextRoot = new ProxyProperties();

        // When
        emptyContextRoot.setGafferContextRoot("");
        slashContextRoot.setGafferContextRoot("/");
        normalContextRoot.setGafferContextRoot("/restv2");

        // Then
        assertEquals(DEFAULT_GAFFER_CONTEXT_ROOT, defaultContextRoot.getGafferContextRoot());
        assertEquals("/", emptyContextRoot.getGafferContextRoot());
        assertEquals("/", slashContextRoot.getGafferContextRoot());
        assertEquals("/restv2", normalContextRoot.getGafferContextRoot());

        assertEquals("http://localhost:8080/rest", defaultContextRoot.getGafferUrl().toExternalForm());
        assertEquals("http://localhost:8080/rest/", defaultContextRoot.getGafferUrl("/").toExternalForm());
        assertEquals("http://localhost:8080/rest/content", defaultContextRoot.getGafferUrl("/content").toExternalForm());

        assertEquals("http://localhost:8080/", emptyContextRoot.getGafferUrl().toExternalForm());
        assertEquals("http://localhost:8080/", emptyContextRoot.getGafferUrl("/").toExternalForm());
        assertEquals("http://localhost:8080/content", emptyContextRoot.getGafferUrl("/content").toExternalForm());

        assertEquals("http://localhost:8080/", slashContextRoot.getGafferUrl().toExternalForm());
        assertEquals("http://localhost:8080/", slashContextRoot.getGafferUrl("/").toExternalForm());
        assertEquals("http://localhost:8080/content", slashContextRoot.getGafferUrl("/content").toExternalForm());
    }
}
