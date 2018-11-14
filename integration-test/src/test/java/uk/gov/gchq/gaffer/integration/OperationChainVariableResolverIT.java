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

package uk.gov.gchq.gaffer.integration;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.VariableDetail;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.SetVariable;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;

import java.util.Arrays;

public class OperationChainVariableResolverIT extends AbstractStoreIT {
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void test() throws OperationException {
        GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(SOURCE_1), new EntitySeed(DEST_2))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final Context context = new Context(getUser());

        CloseableIterable<? extends Element> results = graph.execute(getElements, context);

        ElementUtil.assertElementEquals(Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(SOURCE_1)
                        .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(DEST_2)
                        .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
                ),
                results);
    }

    @Test
    public void shouldPass() throws OperationException {
        final OperationChain<CloseableIterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new SetVariable.Builder()
                        .variableName("testInputVar")
                        .input(new VariableDetail.Builder()
                                .valueClass(String.class)
                                .value(SOURCE_1)
                                .build())
                        .build())
                .then(new GetElements.Builder()
                        .input("${testInputVar}", new EntitySeed(DEST_2))
                        .view(new View.Builder()
                                .entity(TestGroups.ENTITY)
                                .build())
                        .build())
                .build();

        final Context context = new Context(getUser());

        CloseableIterable<? extends Element> results = graph.execute(opChain, context);

        ElementUtil.assertElementEquals(Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(SOURCE_1)
                        .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(DEST_2)
                        .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
                ),
                results);
    }
}
