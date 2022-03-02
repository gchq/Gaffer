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

package uk.gov.gchq.gaffer.graph;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphResultTest {

    private final GetAllElements operation = new GetAllElements();
    private final Context context = new Context();

    @Test
    public void equalsCoverage() {
        // Given When
        final GraphResult<Operation> actual = new GraphResult<>(operation, context);
        final GraphResult<Operation> expected = new GraphResult<>(operation, context);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void toStringCoverage() {
        // Given / When
        final GraphResult<Operation> actual = new GraphResult<>(operation, context);

        // Then
        assertThat(actual.toString())
                .contains("GraphResult[result=uk.gov.gchq.gaffer.operation.impl.get.GetAllElements");
    }
}
