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
package uk.gov.gchq.gaffer.integration.template;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WhileITTemplate extends AbstractStoreIT {

    @GafferTest
    public void shouldRepeatedlyAddElements(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getEmptyGraph();
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
        graph.execute(operation, new User());

        final List<? extends Element> results = Lists.newArrayList(graph.execute(new GetElements.Builder()
                .input("1")
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build(), new User()));

        assertEquals(1, results.size());
        assertEquals(10L, results.get(0).getProperty(TestPropertyNames.COUNT));
    }
}
