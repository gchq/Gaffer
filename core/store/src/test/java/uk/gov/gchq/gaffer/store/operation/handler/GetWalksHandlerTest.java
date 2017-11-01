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
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class GetWalksHandlerTest {

    @Test
    public void shouldHandleNullInput() throws Exception {
        // Given
        final GetElements operations = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();
        final GetWalks operation = new GetWalks.Builder()
                .operations(operations)
                .build();

        final GetWalksHandler handler = new GetWalksHandler();

        // When
        final Iterable<Walk> result = handler.doOperation(operation, null, null);

        // Then
        assertThat(result, is(nullValue()));
    }

    @Test
    public void shouldHandleNullOperations() throws Exception {
        // Given
        final EntitySeed input = new EntitySeed("A");
        final Iterable<GetElements> operations = null;
        final GetWalks operation = new GetWalks.Builder()
                .input(input)
                .operations(operations)
                .build();

        final GetWalksHandler handler = new GetWalksHandler();

        // When
        final Iterable<Walk> result = handler.doOperation(operation, null, null);

        // Then
        assertThat(result, is(new EmptyClosableIterable<>()));
    }

}