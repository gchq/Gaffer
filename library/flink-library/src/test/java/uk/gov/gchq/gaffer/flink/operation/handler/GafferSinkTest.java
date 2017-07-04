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

import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.flink.operation.FlinkTest;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GafferSinkTest {
    @Test
    public void shouldAddElementsToStore() throws Exception {
        // Given
        final Validatable op = mock(Validatable.class);
        final AccumuloStore store = FlinkTest.createStore();
        final Graph graph = new Graph.Builder()
                .store(store)
                .build();
        given(op.isValidate()).willReturn(true);
        given(op.isSkipInvalidElements()).willReturn(false);

        final GafferSink sink = new GafferSink(op, store);

        // When
        sink.invoke(FlinkTest.EXPECTED_ELEMENTS);

        // Then
        FlinkTest.verifyElements(graph);
    }

    @Test
    public void shouldRestartAddElementsIfPauseInIngest() throws Exception {
        // Given
        final Validatable op = mock(Validatable.class);
        final AccumuloStore store = FlinkTest.createStore();
        final Graph graph = new Graph.Builder()
                .store(store)
                .build();
        given(op.isValidate()).willReturn(true);
        given(op.isSkipInvalidElements()).willReturn(false);

        final GafferSink sink = new GafferSink(op, store);

        // When / Then
        sink.invoke(FlinkTest.EXPECTED_ELEMENTS);
        FlinkTest.verifyElements(graph);
        sink.invoke(FlinkTest.EXPECTED_ELEMENTS);

        Thread.sleep(2000);
        final Set<Element> allElements = Sets.newHashSet(graph.execute(new GetAllElements(), new User()));
        Set<Element> allExpectedElements = Sets.newHashSet(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("1")
                        .property(TestPropertyNames.COUNT, 2L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("2")
                        .property(TestPropertyNames.COUNT, 2L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("3")
                        .property(TestPropertyNames.COUNT, 2L)
                        .build()
        );
        assertEquals(allExpectedElements, allElements);
    }

    @Test
    public void shouldAddElementsIfInvokeCalledMultipleTimes() throws Exception {
        // Given
        final long duplicates = 1000;
        final Validatable op = mock(Validatable.class);
        final AccumuloStore store = FlinkTest.createStore();
        final Graph graph = new Graph.Builder()
                .store(store)
                .build();
        given(op.isValidate()).willReturn(true);
        given(op.isSkipInvalidElements()).willReturn(false);

        final GafferSink sink = new GafferSink(op, store);

        // When / Then
        for (int i = 0; i < duplicates; i++) {
            sink.invoke(FlinkTest.EXPECTED_ELEMENTS);
        }

        Thread.sleep(2000);
        final Set<Element> allElements = Sets.newHashSet(graph.execute(new GetAllElements(), new User()));
        Set<Element> allExpectedElements = Sets.newHashSet(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("1")
                        .property(TestPropertyNames.COUNT, duplicates)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("2")
                        .property(TestPropertyNames.COUNT, duplicates)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("3")
                        .property(TestPropertyNames.COUNT, duplicates)
                        .build()
        );
        assertEquals(allExpectedElements, allElements);
    }
}
