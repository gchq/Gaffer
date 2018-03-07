/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ViewParameterDetailTest {

    private static final String EXCEPTION_EXPECTED = "Exception expected";

    @Test
    public void shouldBuildFullViewParameterDetail() {
        // When
        new ViewParameterDetail.Builder()
                .defaultValue(2L)
                .valueClass(Long.class)
                .description("test ParamDetail")
                .required(false)
                .build();

        // Then - No exceptions
    }

    @Test
    public void shouldThrowExceptionWhenDefaultAndRequiredIsSet() {
        // When / Then
        try {
            new ViewParameterDetail.Builder()
                    .defaultValue(2L)
                    .valueClass(Long.class)
                    .description("test ParamDetail")
                    .required(true)
                    .build();
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("required is true but a default value has been provided"));
        }
    }

    @Test
    public void shouldThrowExceptionWithNoClassSet() {
        // When / Then
        try {
            new ViewParameterDetail.Builder()
                    .defaultValue(2L)
                    .description("test paramDetail")
                    .build();
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("class must not be empty"));
        }
    }
}
