/*
 * Copyright 2020-2021 Crown Copyright
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

import org.junit.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CorsIT extends AbstractRestApiIT {
    @Test
    public void shouldBeAbleToRequestFromADifferentOrigin() {
        // Given
        MultiValueMap<String, String> headers = new LinkedMultiValueMap();
        headers.add("Origin", "http://my-ui.com/ui");

        HttpEntity httpEntity = new HttpEntity<>(headers);

        // When
        ResponseEntity<Map> response = request("/properties", HttpMethod.GET, httpEntity, Map.class);

        // Then
        checkResponse(response, 200);
        assertThat(response.getHeaders().getAccessControlAllowOrigin()).isEqualTo("http://my-ui.com/ui");
    }
}
