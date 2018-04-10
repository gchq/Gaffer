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

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class WhileIT extends AbstractStoreIT {

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Test
    public void shouldRepeatedlyAddElements() throws OperationException {
        // Given
        final While operation = new While.Builder<>()
                .operation(new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group(TestGroups.ENTITY)
                                .vertex("1")
                                .property(TestPropertyNames.COUNT, 2L)
                                .property(TestPropertyNames.INT, 2)
                                .property(TestPropertyNames.SET, CollectionUtil.treeSet(""))
                                .build())
                        .build())
                .condition(true)
                .maxRepeats(5)
                .build();

        // When
        graph.execute(operation, getUser());

        final List<? extends Element> results = Lists.newArrayList(graph.execute(new GetElements.Builder()
                .input("1")
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build(), getUser()));

        assertEquals(1, results.size());
        assertEquals(10L, results.get(0).getProperty(TestPropertyNames.COUNT));
    }
}
