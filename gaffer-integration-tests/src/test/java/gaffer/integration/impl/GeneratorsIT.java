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
        final List<DomainObject> results = Lists.newArrayList(graph.execute(opChain));

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                new EntityDomainObject(SOURCE_1, "1", null),
                new EdgeDomainObject(SOURCE_1, DEST_1, false, 1, 1L)
        ));
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void shouldConvertFromDomainObjects() throws OperationException, UnsupportedEncodingException {
        // Given
        final OperationChain<Void> opChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<DomainObject>()
                        .generator(new BasicGenerator())
                        .objects(Arrays.asList(
                                new EntityDomainObject("newVertex", "1", null),
                                new EdgeDomainObject("newSource", "newDest", false, 1, 1L)
                        ))
                        .build())
                .then(new AddElements())
                .build();

        // When - add
        graph.execute(opChain);

        // Then - check they were added correctly
        final List<Element> results = Lists.newArrayList(graph.execute(new GetElementsSeed.Builder<>()
                .addSeed(new EntitySeed("newVertex"))
                .addSeed(new EdgeSeed("newSource", "newDest", false))
                .build()));
        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                new Entity(TestGroups.ENTITY, "newVertex"),
                new Edge(TestGroups.EDGE, "newSource", "newDest", false)
        ));
    }
}
