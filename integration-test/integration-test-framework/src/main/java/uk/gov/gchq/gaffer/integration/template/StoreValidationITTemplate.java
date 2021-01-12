/*
 * Copyright 2016-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.AGE_OFF_TIME;

public class StoreValidationITTemplate extends AbstractStoreIT {
    private static final String VERTEX = "vertex";

    @GafferTest
    @TraitRequirement(StoreTrait.STORE_VALIDATION)
    public void shouldAgeOffDataBasedOnTimestampAndAgeOffFunctionInSchema(final GafferTestCase testcase) throws OperationException, InterruptedException {
        // Given
        Graph graph = testcase.getGraph();
        final User user = new User();
        final long now = System.currentTimeMillis();
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.TIMESTAMP, now);
        entity.putProperty(TestPropertyNames.INT, 5);

        graph.execute(new AddElements.Builder()
                .input(entity)
                .build(), user);

        // When 1 - before age off
        final CloseableIterable<? extends Element> results1 = graph.execute(new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY_2)
                        .build())
                .build(), user);

        // Then 1
        final List<Element> results1List = Lists.newArrayList(results1);
        assertEquals(1, results1List.size());
        assertEquals(VERTEX, ((Entity) results1List.get(0)).getVertex());


        // Wait until after the age off time
        while (System.currentTimeMillis() - now < AGE_OFF_TIME) {
            Thread.sleep(1000L);
        }

        // When 2 - after age off
        final CloseableIterable<? extends Element> results2 = graph.execute(new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY_2)
                        .build())
                .build(), user);

        // Then 2
        final List<Element> results2List = Lists.newArrayList(results2);
        assertTrue(results2List.isEmpty());
    }

    @GafferTest
    @TraitRequirement(StoreTrait.STORE_VALIDATION)
    public void shouldRemoveInvalidElements(final GafferTestCase testcase) throws OperationException {
        // Given
        Graph graph = testcase.getGraph();
        final User user = new User();
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // add elements but skip the validation
        graph.execute(new AddElements.Builder()
                .input(Collections.<Element>singleton(entity))
                .validate(false)
                .build(), user);

        // When
        final CloseableIterable<? extends Element> results1 = graph.execute(new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY_2)
                        .build())
                .build(), user);

        // Then
        final List<Element> results1List = Lists.newArrayList(results1);
        assertTrue(results1List.isEmpty());
    }
}