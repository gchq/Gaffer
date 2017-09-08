/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.flink.operation.handler;

import org.junit.Test;

import uk.gov.gchq.gaffer.flink.operation.FlinkTest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GafferOutputTest {
    @Test
    public void shouldDelegateOpenToGafferAddedInitialise() throws Exception {
        // Given
        final GafferAdder adder = mock(GafferAdder.class);
        final GafferOutput output = new GafferOutput(adder);

        // When
        output.open(1, 2);

        // Then
        verify(adder).initialise();
    }

    @Test
    public void shouldDelegateWriteRecordToGafferAddedInitialise() throws Exception {
        // Given
        final GafferAdder adder = mock(GafferAdder.class);
        final GafferOutput output = new GafferOutput(adder);

        // When
        output.writeRecord(FlinkTest.EXPECTED_ELEMENTS);

        // Then
        verify(adder).add(FlinkTest.EXPECTED_ELEMENTS);
    }
}
