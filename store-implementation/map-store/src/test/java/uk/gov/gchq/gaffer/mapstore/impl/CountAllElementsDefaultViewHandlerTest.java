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

package uk.gov.gchq.gaffer.mapstore.impl;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.operation.CountAllElementsDefaultView;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
class CountAllElementsDefaultViewHandlerTest {

    @Test
    void testCountAllElementsDefaultViewHandler() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(GetAllElementsHandlerTest.getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final CountAllElementsDefaultView countAllElementsDefaultView = new CountAllElementsDefaultView();
        final Long result = graph.execute(countAllElementsDefaultView, new User());

        // Then
        assertThat((long) result).isEqualTo((long) GetAllElementsHandlerTest.getElements().size());
    }

    @Test
    void shouldApplyVisibilityTraitToOperationResults() throws OperationException {
        VisibilityTest.executeOperation(
                new CountAllElementsDefaultView(),
                VisibilityTest::elementIterableResultSizeConsumer);
    }
}
