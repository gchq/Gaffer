/*
 * Copyright 2016 Crown Copyright
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
package gaffer.integration.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.integration.AbstractStoreIT;
import gaffer.integration.TraitRequirement;
import gaffer.integration.domain.DomainObject;
import gaffer.integration.domain.EdgeDomainObject;
import gaffer.integration.domain.EntityDomainObject;
import gaffer.integration.generators.BasicGenerator;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetElementsSeed;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.store.StoreTrait;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

public class GeneratorsIT extends AbstractStoreIT {
    private static final String NEW_SOURCE = "newSource";
    private static final String NEW_DEST = "newDest";
    private static final String NEW_VERTEX = "newVertex";

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void shouldConvertToDomainObjects() throws OperationException, UnsupportedEncodingException {
        // Given
        final OperationChain<Iterable<DomainObject>> opChain = new OperationChain.Builder()
                .first(new GetRelatedElements.Builder<>()
                        .addSeed(new EntitySeed(SOURCE_1))
                        .build())
                .then(new GenerateObjects.Builder<Element, DomainObject>()
                        .generator(new BasicGenerator())
                        .build())
                .build();

        // When
        final List<DomainObject> results = Lists.newArrayList(graph.execute(opChain, getUser()));

        final EntityDomainObject entityDomainObject = new EntityDomainObject(SOURCE_1, "3", null);
        final EdgeDomainObject edgeDomainObject = new EdgeDomainObject(SOURCE_1, DEST_1, false, 1, 1L);

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                entityDomainObject, edgeDomainObject));
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void shouldConvertFromDomainObjects() throws OperationException, UnsupportedEncodingException {
        // Given
        final OperationChain<Void> opChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<DomainObject>()
                        .generator(new BasicGenerator())
                        .objects(Arrays.asList(
                                new EntityDomainObject(NEW_VERTEX, "1", null),
                                new EdgeDomainObject(NEW_SOURCE, NEW_DEST, false, 1, 1L)
                        ))
                        .build())
                .then(new AddElements())
                .build();

        // When - add
        graph.execute(opChain, getUser());

        // Then - check they were added correctly
        final List<Element> results = Lists.newArrayList(graph.execute(new GetElementsSeed.Builder<>()
                .addSeed(new EntitySeed(NEW_VERTEX))
                .addSeed(new EdgeSeed(NEW_SOURCE, NEW_DEST, false))
                .build(), getUser()));

        final Edge expectedEdge = new Edge(TestGroups.EDGE, NEW_SOURCE, NEW_DEST, false);
        expectedEdge.putProperty(TestPropertyNames.INT, 1);
        expectedEdge.putProperty(TestPropertyNames.COUNT, 1L);

        final Entity expectedEntity = new Entity(TestGroups.ENTITY, NEW_VERTEX);
        expectedEntity.putProperty(TestPropertyNames.STRING, "1");

        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                expectedEntity, expectedEdge));
    }
}
