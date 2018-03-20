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
package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

import static org.junit.Assert.assertEquals;

public class WhileSimpleIT extends AbstractStoreIT {

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Test
    public void shouldRepeatedlyAddElements() throws OperationException {
        // Given
        final While operation = new While.Builder()
                .operation(new AddElements.Builder()
                        .input(new Edge.Builder()
                                .group("testEdge")
                                .source("src")
                                .dest("dest")
                                .directed(true)
                                .property("count", 2)
                                .build())
                        .build())
                .condition(true)
                .maxRepeats(5)
                .build();

        // When
        graph.execute(operation, getUser());

        final Iterable<? extends Element> results = graph.execute(new GetAllElements(), getUser());

        // Then
        assertEquals(5, Iterables.size(results));
    }
}
