/*
 * Copyright 2023-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

import java.io.InputStream;
import java.util.Properties;

@ExtendWith(SpringExtension.class)
@WebMvcTest(value = VersionController.class)
class VersionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void sendRequestAndCheckForValidVersion() throws Exception {
        // Read the version properties file test resource to compare the endpoint returned one to
        InputStream fileStream = VersionControllerTest.class.getClassLoader().getResourceAsStream("version.properties");
        Properties versionProps = new Properties();
        versionProps.load(fileStream);

        // Perform mock request to the endpoint
        RequestBuilder requestBuilder = MockMvcRequestBuilders
            .get("/rest/graph/version")
            .accept(TEXT_PLAIN_VALUE);
        MvcResult result = mockMvc.perform(requestBuilder).andReturn();

        // Ensure OK response
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Validate the returned string matches a valid version regex and test resource
        String resultString = result.getResponse().getContentAsString();
        assertThat(resultString)
            .matches("(?!\\.)(\\d+(\\.\\d+)+)(?:[-.][A-Z]+)?(?![\\d.])$")
            .isEqualTo(versionProps.getProperty("gaffer.version"));
    }
}
