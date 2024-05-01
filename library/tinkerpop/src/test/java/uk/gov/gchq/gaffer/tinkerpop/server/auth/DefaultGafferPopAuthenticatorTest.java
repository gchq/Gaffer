/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.server.auth;

import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@TestInstance(Lifecycle.PER_CLASS)
class DefaultGafferPopAuthenticatorTest {
    private DefaultGafferPopAuthenticator authenticator;

    @BeforeAll
    void setup() {
        authenticator = new DefaultGafferPopAuthenticator();
    }

    @Test
    void shouldAlwaysRequireAuthentication() {
        assertThat(authenticator.requireAuthentication()).isTrue();
    }

    @Test
    void shouldCreateNewPlainTextSaslNegotiator() {
        // Given
        final Authenticator.SaslNegotiator negotiator1 = authenticator.newSaslNegotiator(null);
        final Authenticator.SaslNegotiator negotiator2 = authenticator.newSaslNegotiator(null);

        // Then
        assertThat(negotiator1).isNotEqualTo(negotiator2);
    }

    @Test
    void shouldAuthenticateWithPlainText() throws AuthenticationException {
        final String user = "test";
        final String pass = "pass";
        final Map<String, String> credentials = new HashMap<>();
        credentials.put("username", user);
        credentials.put("password", pass);

        assertThat(authenticator.authenticate(credentials).getName()).isEqualTo(user);
    }

    @Test
    void shouldParseSaslPlainTextResponse() throws AuthenticationException, IOException {
        // Given
        final Authenticator.SaslNegotiator negotiator = authenticator.newSaslNegotiator(null);
        final String user = "test";
        final String pass = "pass";

        // Create a byte array for the response, this is representative of what the gremlin server is passed
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final byte[] nul = new byte[] {0};
        stream.write(nul);
        stream.write(user.getBytes());
        stream.write(nul);
        stream.write(pass.getBytes());

        // When
        negotiator.evaluateResponse(stream.toByteArray());

        // Then
        assertThat(negotiator.isComplete()).isTrue();
        assertThat(negotiator.getAuthenticatedUser().getName()).isEqualTo(user);
    }
}
