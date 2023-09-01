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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@ExtendWith(SpringExtension.class)
@WebMvcTest(value = VersionController.class)
class VersionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void sendRequestAndCheckForValidVersion() throws Exception {
        // Perform mock request to the endpoint
        RequestBuilder requestBuilder = MockMvcRequestBuilders
            .get("/graph/version")
            .accept(TEXT_PLAIN_VALUE);
        MvcResult result = mockMvc.perform(requestBuilder).andReturn();

        // Validate the returned string matches a valid version regex
        String resultString = result.getResponse().getContentAsString();
        //System.out.println(result.getResponse().toString());
        assertTrue(
            resultString.matches("(?!\\.)(\\d+(\\.\\d+)+)(?:[-.][A-Z]+)?(?![\\d.])$"),
            "The response from the endpoint is not a valid version string, output: " + resultString);

    }
}
