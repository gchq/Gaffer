/*
 * Copyright 2017-2020 Crown Copyright
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.store.Context;

import java.util.Arrays;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class ToEntitySeedsHandlerTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldConvertVerticesToEntitySeeds(@Mock final ToEntitySeeds operation) throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";

        final Iterable<Object> originalResults = Arrays.asList(vertex1, vertex2);
        final ToEntitySeedsHandler handler = new ToEntitySeedsHandler();

        given(operation.getInput()).willReturn((Iterable) originalResults);

        // When
        final Iterable<EntitySeed> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertThat(results).containsOnly(new EntitySeed(vertex1), new EntitySeed(vertex2));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldBeAbleToIterableOverTheResultsMultipleTimes(@Mock final ToEntitySeeds operation)
            throws OperationException {
        // Given
        final Object vertex1 = "vertex1";
        final Object vertex2 = "vertex2";

        final Iterable<Object> originalResults = Arrays.asList(vertex1, vertex2);
        final ToEntitySeedsHandler handler = new ToEntitySeedsHandler();

        given(operation.getInput()).willReturn((Iterable) originalResults);

        // When
        final Iterable<EntitySeed> results = handler.doOperation(operation, new Context(), null);

        // Then
        final Set<Object> set1 = Sets.newHashSet(results);
        final Set<Object> set2 = Sets.newHashSet(results);
        assertThat(set1)
                .isEqualTo(Sets.newHashSet(new EntitySeed(vertex1), new EntitySeed(vertex2)))
                .isEqualTo(set2);
    }

    @Test
    public void shouldHandleNullInput(@Mock final ToEntitySeeds operation) throws OperationException {
        // Given
        final ToEntitySeedsHandler handler = new ToEntitySeedsHandler();

        given(operation.getInput()).willReturn(null);

        // When
        final Iterable<EntitySeed> results = handler.doOperation(operation, new Context(), null);

        // Then
        assertThat(results).isNull();
    }
}
