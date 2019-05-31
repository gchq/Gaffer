/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.named.operation;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ParameterDetailTest {

    @Test
    public void shouldBuildFullParameterDetailWithOptions() {
        //Given
        List options = Arrays.asList("option1", "option2", "option3");

        // When
        new ParameterDetail.Builder()
                .defaultValue(2L)
                .valueClass(Long.class)
                .description("test ParamDetail")
                .required(false)
                .options(options)
                .build();

        // Then - No exceptions
    }

    @Test
    public void shouldBuildFullParameterDetailWithOptionsOfDifferentTypes() {
        //Given
        List options = Arrays.asList("option1", 2, true);

        // When
        new ParameterDetail.Builder()
                .defaultValue(2L)
                .valueClass(Long.class)
                .description("test ParamDetail")
                .required(false)
                .options(options)
                .build();

        // Then - No exceptions
    }
}
