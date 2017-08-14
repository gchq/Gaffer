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

import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.store.Context;
import java.util.Arrays;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ToEntitySeedsHandlerTest {

    @Test
    public void shouldConvertVerticesToEntitySeeds() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";

        final Iterable originalResults = new WrappedCloseableIterable<>(Arrays.asList(vertex1, vertex2));
        final ToEntitySeedsHandler handler = new ToEntitySeedsHandler();
        final ToEntitySeeds operation = mock(ToEntitySeeds.class);

        given(operation.getInput()).willReturn(originalResults);

        //When
        final Iterable<EntitySeed> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(results, containsInAnyOrder(new EntitySeed(vertex1), new EntitySeed(vertex2)));
    }

    @Test
    public void shouldBeAbleToIterableOverTheResultsMultipleTimes() throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";

        final Iterable originalResults = new WrappedCloseableIterable<>(Arrays.asList(vertex1, vertex2));
        final ToEntitySeedsHandler handler = new ToEntitySeedsHandler();
        final ToEntitySeeds operation = mock(ToEntitySeeds.class);

        given(operation.getInput()).willReturn(originalResults);

        //When
        final Iterable<EntitySeed> results = handler.doOperation(operation, new Context(), null);

        //Then
        final Set<Object> set1 = Sets.newHashSet(results);
        final Set<Object> set2 = Sets.newHashSet(results);
        assertEquals(Sets.newHashSet(new EntitySeed(vertex1), new EntitySeed(vertex2)), set1);
        assertEquals(set1, set2);
    }

    @Test
    public void shouldHandleNullInput() throws OperationException {
        // Given
        final ToEntitySeedsHandler handler = new ToEntitySeedsHandler();
        final ToEntitySeeds operation = mock(ToEntitySeeds.class);

        given(operation.getInput()).willReturn(null);

        //When
        final Iterable<EntitySeed> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(results, is(nullValue()));
    }

}
