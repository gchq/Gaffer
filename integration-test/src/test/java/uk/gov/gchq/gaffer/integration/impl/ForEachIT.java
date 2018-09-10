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

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.ForEach;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertTrue;

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
    }

    @Test
    public void shouldReturnEmptyIterableWithOperationThatDoesntImplementInput() {

    }

    @Test
    public void shouldExecuteForEachOperationOnGetElementsWithValidResults() throws OperationException {
        final ForEach<List<ElementSeed>, Iterable<Element>> op = new ForEach.Builder<List<ElementSeed>, Iterable<Element>>()
                .operation(new GetElements.Builder().view(new View()).build())
                .input(Arrays.asList(new EdgeSeed(SOURCE_DIR_1, DEST_DIR_1, true)))
                .build();

        Iterable<Element> results = graph.execute(op, user);

        ElementUtil.assertElementEquals(new HashSet<>(), results);
    }

    @Test
    public void shouldExecuteForEachOperationOnCountWithValidResults() throws OperationException {
        // Given
        final Iterable<List> inputIterable = Arrays.asList(Arrays.asList("1", "2", "3"));
        final ForEach<List<List<String>>, Iterable<Long>> op = new ForEach.Builder<List<List<String>>, Iterable<Long>>()
                .input(inputIterable)
                .operation(new Count<>())
                .build();

        // When
        Iterable<Long> output = graph.execute(op, user);

        // Then
        assertTrue(output.iterator().next().equals(3L));
    }

    @Test
    public void shouldExecuteForEachOperationOnInputWithEmptyIterable() throws OperationException {
        final ForEach<List<ElementSeed>, Iterable<Element>> op = new ForEach.Builder<List<ElementSeed>, Iterable<Element>>()
                .operation(new GetElements.Builder().view(new View()).build())
                .input(Arrays.asList(new EdgeSeed("doesNotExist", "doesNotExist", true)))
                .build();

        Iterable<Element> results = graph.execute(op, user);

        ElementUtil.assertElementEquals(new HashSet<>(), results);
    }
}
