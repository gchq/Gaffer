/*
 * Copyright 2017-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ToStringBuilderTest {

    @BeforeEach
    public void setUp() {
        clearDebugModeProperty();
    }

    @AfterEach
    public void after() {
        clearDebugModeProperty();
    }

    @Test
    void testDebugOffToStringBuilder() {
        setDebugMode("false");
        ToStringBuilder toStringBuilder = new ToStringBuilder("Test String");

        assertThat(toStringBuilder.getStyle()).isEqualTo(ToStringBuilder.SHORT_STYLE);
    }

    @Test
    void testDebugOnToStringBuilder() {
        setDebugMode("true");
        ToStringBuilder toStringBuilder = new ToStringBuilder("Test String");

        assertThat(toStringBuilder.getStyle()).isEqualTo(ToStringBuilder.FULL_STYLE);
    }

    private void setDebugMode(final String value) {
        System.setProperty(DebugUtil.DEBUG, value);
        DebugUtil.updateDebugMode();
    }

    private void clearDebugModeProperty() {
        System.clearProperty(DebugUtil.DEBUG);
        DebugUtil.updateDebugMode();
    }
}
