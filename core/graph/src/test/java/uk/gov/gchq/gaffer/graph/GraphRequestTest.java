/*
 * Copyright 2020 Crown Copyright
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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphRequestTest {

    @Test
    public void graphRequestEqualsCoverage() {
        // Given
        final GetAllElements operation = new GetAllElements();
        final Context context = new Context();

        // When
        final GraphRequest<Operation> request = new GraphRequest<Operation>(operation, context);
        final GraphRequest<Operation> expected = new GraphRequest<Operation>(operation, context);

        // Then
        assertEquals(expected, request);
    }
}
