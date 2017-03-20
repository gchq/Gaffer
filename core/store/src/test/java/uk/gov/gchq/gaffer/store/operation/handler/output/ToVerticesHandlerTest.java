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

package uk.gov.gchq.gaffer.store.operation.handler.output;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.store.Context;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ToVerticesHandlerTest {

    @Test
    public void shouldConvertElementSeedsToVertices() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";

        final List<ElementId> elementIds = Arrays.asList(new EntitySeed(vertex1), new EntitySeed(vertex2));

        final ToVerticesHandler handler = new ToVerticesHandler();
        final ToVertices operation = mock(ToVertices.class);

        given(operation.getInput()).willReturn(elementIds);

        //When
        final Iterable<Object> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(results, containsInAnyOrder(vertex1, vertex2));
    }
}
