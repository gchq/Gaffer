/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.MapGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import uk.gov.gchq.gaffer.store.Context;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ToMapHandlerTest {

    @Test
    public void shouldConvertElementToMap() throws OperationException {
        // Given
        final Entity entity = new Entity.Builder().group(TestGroups.ENTITY)
                                                  .vertex(1)
                                                  .build();

        final Map<String, Object> originalMap = new HashMap<>(1);
        originalMap.put("group", TestGroups.ENTITY);
        originalMap.put("vertex", 1);

        final MapGenerator generator = new MapGenerator.Builder()
                .group("group")
                .vertex("vertex")
                .source("source")
                .destination("destination")
                .build();

        final Iterable originalResults = new WrappedCloseableIterable<>(Collections.singleton(entity));
        final ToMapHandler handler = new ToMapHandler();
        final ToMap operation = mock(ToMap.class);

        given(operation.getInput()).willReturn(originalResults);
        given(operation.getElementGenerator()).willReturn(generator);

        //When
        final Iterable<? extends Map<String, Object>> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(results, contains(originalMap));
    }

    @Test
    public void shouldHandleNullInput() throws OperationException {
        // Given
        final ToMapHandler handler = new ToMapHandler();
        final ToMap operation = mock(ToMap.class);

        given(operation.getInput()).willReturn(null);

        //When
        final Iterable<? extends Map<String, Object>> results = handler.doOperation(operation, new Context(), null);

        //Then
        assertThat(results, is(nullValue()));
    }
}
