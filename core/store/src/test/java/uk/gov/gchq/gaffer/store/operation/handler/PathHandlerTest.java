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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Path;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

public class PathHandlerTest {

    @Test
    public void shouldHandleNullInput() throws Exception {
        // Given
        final GetElements operations = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();
        final Path operation = new Path.Builder()
                .operations(operations)
                .build();

        final PathHandler handler = new PathHandler();

        // When
        final Iterable<Iterable<Edge>> result = handler.doOperation(operation, null, null);

        // Then
        assertThat(result, is(nullValue()));
    }

    @Test
    public void shouldHandleEmptyInput() throws Exception {
        // Given
        final List<EntitySeed> input = Collections.emptyList();
        final GetElements operations = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();
        final Path operation = new Path.Builder()
                .input(input)
                .operations(operations)
                .build();

        final PathHandler handler = new PathHandler();

        // When
        final Iterable<Iterable<Edge>> result = handler.doOperation(operation, null, null);

        // Then
        assertThat(result, is(nullValue()));
    }

    @Test
    public void shouldHandleNullOperations() throws Exception {
        // Given
        final EntitySeed input = new EntitySeed("A");
        final Iterable<GetElements> operations = null;
        final Path operation = new Path.Builder()
                .input(input)
                .operations(operations)
                .build();

        final PathHandler handler = new PathHandler();

        // When
        final Iterable<Iterable<Edge>> result = handler.doOperation(operation, null, null);

        // Then
        assertThat(result, is(nullValue()));
    }

    @Test
    public void shouldHandleInvalidViewOnGetElementsOperation() throws Exception {
        // Given
        final EntitySeed input = new EntitySeed("A");
        final GetElements operations = new GetElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();
        final Path operation = new Path.Builder()
                .input(input)
                .operations(operations)
                .build();

        final PathHandler handler = new PathHandler();

        // When
        try {
            final Iterable<Iterable<Edge>> result = handler.doOperation(operation, null, null);

            fail("Expected exception not thrown.");
        } catch (final OperationException e) {
            // Then
            assertThat(e.getMessage(), containsString("must not contain Entities."));
        }
    }

}