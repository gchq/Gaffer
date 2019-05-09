/*
 * Copyright 2016-2019 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CountGroupsIT extends AbstractStoreIT {
    private static final String VERTEX = "vertex";

    @Override
    public void _setup() throws Exception {
        addDefaultElements();
    }

    @Test
    public void shouldCountGroupsOfElements() throws OperationException {
        // Given
        final User user = new User();
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // When
        final GroupCounts counts = graph.execute(new Builder()
                .first(new GetAllElements())
                .then(new CountGroups())
                .build(), user);

        // Then
        assertEquals(1, counts.getEntityGroups().size());
        assertEquals(getEntities().size(), (int) counts.getEntityGroups().get(TestGroups.ENTITY));
        assertEquals(1, counts.getEdgeGroups().size());
        assertEquals(getEdges().size(), (int) counts.getEdgeGroups().get(TestGroups.EDGE));
        assertFalse(counts.isLimitHit());
    }

    @Test
    public void shouldCountGroupsOfElementsWhenLessElementsThanLimit() throws OperationException {
        // Given
        final User user = new User();
        final Integer limit = getEntities().size() + getEdges().size() + 1;
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // When
        final GroupCounts counts = graph.execute(new Builder()
                .first(new GetAllElements())
                .then(new CountGroups(limit))
                .build(), user);

        // Then
        assertEquals(1, counts.getEntityGroups().size());
        assertEquals(getEntities().size(), (int) counts.getEntityGroups().get(TestGroups.ENTITY));
        assertEquals(1, counts.getEdgeGroups().size());
        assertEquals(getEdges().size(), (int) counts.getEdgeGroups().get(TestGroups.EDGE));
        assertFalse(counts.isLimitHit());
    }

    @Test
    public void shouldCountGroupsOfElementsWhenMoreElementsThanLimit() throws OperationException {
        // Given
        final User user = new User();
        final int limit = 5;
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // When
        final GroupCounts counts = graph.execute(new Builder()
                .first(new GetAllElements())
                .then(new CountGroups(limit))
                .build(), user);

        // Then
        int totalCount = null != counts.getEntityGroups().get(TestGroups.ENTITY) ? counts.getEntityGroups().get(TestGroups.ENTITY) : 0;
        totalCount += null != counts.getEdgeGroups().get(TestGroups.EDGE) ? counts.getEdgeGroups().get(TestGroups.EDGE) : 0;
        assertEquals(limit, totalCount);
    }
}
