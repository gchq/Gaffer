/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class ViewParameterDetailTest {

    @Test
    public void shouldBuildFullViewParameterDetail() {
        assertThatNoException().isThrownBy(() -> new ViewParameterDetail.Builder()
                .defaultValue(2L)
                .valueClass(Long.class)
                .description("test ParamDetail")
                .required(false)
                .build());
    }

    @Test
    public void shouldThrowExceptionWhenDefaultAndRequiredIsSet() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ViewParameterDetail.Builder()
                        .defaultValue(2L)
                        .valueClass(Long.class)
                        .description("test ParamDetail")
                        .required(true)
                        .build())
                .withMessage("required is true but a default value has been provided");
    }

    @Test
    public void shouldThrowExceptionWithNoClassSet() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ViewParameterDetail.Builder()
                        .defaultValue(2L)
                        .description("test paramDetail")
                        .build()).withMessage("class must not be empty");
    }
}
