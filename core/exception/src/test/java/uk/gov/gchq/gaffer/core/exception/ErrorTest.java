/*
 * Copyright 2019-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.core.exception;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.DebugUtil;
import uk.gov.gchq.gaffer.core.exception.Error.ErrorBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ErrorTest {

    private static final String DETAILED_MSG = "detailedMessage";
    private static final String SIMPLE_MSG = "simpleMessage";

    @BeforeEach
    public void setUp() throws Exception {
        clearDebugModeProperty();
    }

    @AfterEach
    public void after() {
        clearDebugModeProperty();
    }

    @Test
    public void shouldNotBuildDetailedMessage() {
        setDebugMode("false");

        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        assertNotEquals(DETAILED_MSG, error.getDetailMessage());
    }

    @Test
    public void shouldNotBuildDetailedMessageWithMissingPropertyFlag() {
        clearDebugModeProperty();

        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        assertNotEquals(DETAILED_MSG, error.getDetailMessage());
    }

    @Test
    public void shouldNotBuildDetailedMessageWithIncorrectPropertyFlag() {
        setDebugMode("wrong");

        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        assertNotEquals(DETAILED_MSG, error.getDetailMessage());
    }

    @Test
    public void shouldBuildDetailedAndSimpleMessageWhenDebugPropertyIsTrue() {
        setDebugMode("true");

        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        assertEquals(DETAILED_MSG, error.getDetailMessage());
        assertEquals(SIMPLE_MSG, error.getSimpleMessage());
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
