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

import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * The default authenticator class for GafferPop, this should not
 * be used in production as it allows all user and password combinations.
 * The class is intended as a template for an instance specific class
 * that hooks into a proper authorisation mechanism such as LDAP etc.
 */
public class DefaultGafferPopAuthenticator implements Authenticator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultGafferPopAuthenticator.class);

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    public void setup(final Map<String, Object> config) {
        // Nothing to do
    }

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        return new PlainTextSaslAuthenticator();
    }


    @Override
    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        // Get the username from the credentials set by the SASL negotiator
        final String username = credentials.get("username");
        return new AuthenticatedUser(username);
    }

    /**
     * Very simple SASL authenticator that will just extract username and password
     * from plain text.
     */
    private class PlainTextSaslAuthenticator implements Authenticator.SaslNegotiator {
        private static final byte NUL = 0;
        private boolean complete = false;
        private String username;
        private String password;

        @Override
        public byte[] evaluateResponse(final byte[] clientResponse) throws AuthenticationException {
            decodeCredentials(clientResponse);
            complete = true;
            return new byte[0];
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
            if (!complete) {
                throw new AuthenticationException("SASL negotiation not complete");
            }
            final Map<String, String> credentials = new HashMap<>();
            credentials.put("username", username);
            credentials.put("password", password);
            return authenticate(credentials);
        }

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is :
         *
         * <pre>
         * authzIdNULauthnIdNULpasswordNUL
         * </pre>
         *
         * @param bytes encoded credentials string sent by the client
         * @throws AuthenticationException If issue decoding
         */
        private void decodeCredentials(final byte[] bytes) throws AuthenticationException {
            byte[] user = null;
            byte[] pass = null;
            int end = bytes.length;
            // Loop over the byte array to extract the user and password
            for (int i = bytes.length - 1; i >= 0; i--) {
                if (bytes[i] != NUL) {
                    continue;
                }

                if (pass == null) {
                    pass = Arrays.copyOfRange(bytes, i + 1, end);
                } else if (user == null) {
                    user = Arrays.copyOfRange(bytes, i + 1, end);
                }
                end = i;
            }

            if (user == null) {
                throw new AuthenticationException("Authentication ID must not be null");
            }
            if (pass == null) {
                throw new AuthenticationException("Password must not be null");
            }

            username = new String(user, StandardCharsets.UTF_8);
            password = new String(pass, StandardCharsets.UTF_8);
        }
    }

}
