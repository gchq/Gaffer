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

package uk.gov.gchq.gaffer.rest.service.v2;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;

import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import static org.assertj.core.api.Assertions.assertThat;

class VersionServiceV2Test extends JerseyTest {

    @Override
    public Application configure() {
        return new ResourceConfig(VersionServiceV2.class);
    }

    @Test
    void sendRequestAndCheckForValidVersion() {
        // Send response to the endpoint
        Response response = target("/graph/version").request().get();

        // Validate the response
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getEntity().toString()).isNotNull();

        // Test the version is correct
        String responseString = response.readEntity(String.class);
        assertThat(responseString).matches("(?!\\.)(\\d+(\\.\\d+)+)(?:[-.][A-Z]+)?(?![\\d.])$");
    }


}
