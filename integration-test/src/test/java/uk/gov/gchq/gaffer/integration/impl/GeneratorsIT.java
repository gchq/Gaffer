/*
 * Copyright 2016-2024 Crown Copyright
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
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.domain.DomainObject;
import uk.gov.gchq.gaffer.integration.domain.EdgeDomainObject;
import uk.gov.gchq.gaffer.integration.domain.EntityDomainObject;
import uk.gov.gchq.gaffer.integration.generators.BasicElementGenerator;
import uk.gov.gchq.gaffer.integration.generators.BasicObjectGenerator;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class GeneratorsIT extends AbstractStoreIT {
    private static final String NEW_SOURCE = "newSource";
    private static final String NEW_DEST = "newDest";
    private static final String NEW_VERTEX = "newVertex";

    @Override
    public void _setup() throws Exception {
        addDefaultElements();
    }

    @Test
    public void shouldConvertToDomainObjects() throws OperationException {
        // Given
        final OperationChain<Iterable<? extends DomainObject>> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(SOURCE_1))
                        .build())
                .then(new GenerateObjects.Builder<DomainObject>()
                        .generator(new BasicObjectGenerator())
                        .build())
                .build();

        // When
        final List<DomainObject> results = Lists.newArrayList(graph.execute(opChain, getUser()));

        final EntityDomainObject entityDomainObject = new EntityDomainObject(SOURCE_1, "3", null);
        final EdgeDomainObject edgeDomainObject = new EdgeDomainObject(SOURCE_1, DEST_1, false, 1, 1L);

        // Then
        assertThat(results)
                .hasSize(2)
                .contains(entityDomainObject, edgeDomainObject);
    }

    @Test
    public void shouldConvertFromDomainObjects() throws OperationException {
        // Given
        final OperationChain<Void> opChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<DomainObject>()
                        .generator(new BasicElementGenerator())
                        .input(new EntityDomainObject(NEW_VERTEX, "1", null),
                                new EdgeDomainObject(NEW_SOURCE, NEW_DEST, true, 1, 1L))
                        .build())
                .then(new AddElements())
                .build();

        // When - add
        graph.execute(opChain, getUser());

        // Then - check they were added correctly
        final List<Element> results = Lists.newArrayList(graph.execute(new GetElements.Builder()
                .input(new EntitySeed(NEW_VERTEX), new EdgeSeed(NEW_SOURCE, NEW_DEST, true))
                .build(), getUser()));

        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(NEW_SOURCE)
                .dest(NEW_DEST)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build();
        expectedEdge.putProperty(TestPropertyNames.INT, 1);
        expectedEdge.putProperty(TestPropertyNames.COUNT, 1L);

        final Entity expectedEntity = new Entity(TestGroups.ENTITY, NEW_VERTEX);
        expectedEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("1"));

        ElementUtil.assertElementEqualsIncludingMatchedVertex(Arrays.asList(expectedEntity, expectedEdge), results);
    }
}
