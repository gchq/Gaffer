/*
 * Copyright 2018 Crown Copyright
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


package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.generator.CsvGenerator;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.ForEach;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.operation.impl.output.ToSingletonList;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.function.ToInteger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ForEachIT extends AbstractStoreIT {
    final User user = new User();

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldReturnEmptyIterableWithOperationThatDoesntImplementOutput() throws OperationException {
        // Given
        final ForEach<ElementSeed, Element> op = new ForEach.Builder<ElementSeed, Element>()
                .operation(new DiscardOutput.Builder().build())
                .input(Collections.singletonList(new EdgeSeed(SOURCE_DIR_1, DEST_DIR_1, true)))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(op, user);

        // Then
        ElementUtil.assertElementEquals(Sets.newHashSet((ElementId) null), results);
    }

    @Test
    public void shouldExecuteForEachOperationOnCountWithValidResults() throws OperationException {
        // Given
        final List<List<String>> inputIterable = Arrays.asList(Arrays.asList("1", "2", "3"), Arrays.asList("4", "5"), Arrays.asList());
        final ForEach<List<String>, Long> op = new ForEach.Builder<List<String>, Long>()
                .input(inputIterable)
                .operation(new OperationChain.Builder().first(new Count<>()).then(new Map.Builder<>().first(new ToInteger()).build()).build())
                .build();

        // When
        final Iterable<? extends Long> output = graph.execute(op, user);

        // Then
        assertEquals(Arrays.asList(3, 2, 0), Lists.newArrayList(output));
    }

    @Test
    public void shouldExecuteForEachOperationOnGetElementsWithValidResults() throws OperationException {
        // Given
        final ForEach<String, Iterable<String>> op = new ForEach.Builder<String, Iterable<String>>()
                .input(Collections.singletonList(SOURCE_DIR_1))
                .operation(
                        new OperationChain.Builder()
                                .first(new ToSingletonList<>())
                                .then(new ToEntitySeeds())
                                .then(new GetElements())
                                .then(new ToList<Element>())
                                .then(new ToCsv.Builder()
                                        .includeHeader(false)
                                        .generator(new CsvGenerator.Builder()
                                                .vertex("vertex")
                                                .destination("dest")
                                                .build())
                                        .build())
                                .build())
                .build();

        // When
        final List<Iterable<String>> results = Lists.newArrayList(graph.execute(op, user));

        // Then
        assertEquals(1, results.size());
        assertEquals(
                Sets.newHashSet(SOURCE_DIR_1 + ",", "," + DEST_DIR_1),
                Sets.newHashSet(results.get(0))
        );
    }

    @Test
    public void shouldExecuteForEachOperationOnGetElementsWithEmptyIterable() throws OperationException {
        // Given
        final ForEach<ElementSeed, Iterable<String>> op = new ForEach.Builder<ElementSeed, Iterable<String>>()
                .input(Collections.singletonList(new EdgeSeed("doesNotExist", "doesNotExist", true)))
                .operation(
                        new OperationChain.Builder()
                                .first(new ToSingletonList<EntitySeed>())
                                .then(new GetElements())
                                .then(new ToCsv.Builder()
                                        .includeHeader(false)
                                        .generator(new CsvGenerator.Builder()
                                                .vertex("vertex")
                                                .destination("dest")
                                                .build())
                                        .build())
                                .build())
                .build();

        // When
        final List<Iterable<String>> results = Lists.newArrayList(graph.execute(op, user));

        // Then
        assertEquals(1, results.size());
        assertEquals(Collections.emptyList(), Lists.newArrayList(results.get(0)));
    }
}
