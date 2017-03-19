package uk.gov.gchq.gaffer.integration.impl;

import org.junit.Before;
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
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldCountGroupsOfElements() throws OperationException, InterruptedException {
        // Given
        final User user = new User();
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // When
        final GroupCounts counts = graph.execute(new Builder()
                .first(new GetAllElements<>())
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
    public void shouldCountGroupsOfElementsWhenLessElementsThanLimit() throws OperationException, InterruptedException {
        // Given
        final User user = new User();
        final Integer limit = getEntities().size() + getEdges().size() + 1;
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // When
        final GroupCounts counts = graph.execute(new Builder()
                .first(new GetAllElements<>())
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
    public void shouldCountGroupsOfElementsWhenMoreElementsThanLimit() throws OperationException, InterruptedException {
        // Given
        final User user = new User();
        final int limit = 5;
        final Entity entity = new Entity(TestGroups.ENTITY_2, VERTEX);
        entity.putProperty(TestPropertyNames.INT, 100);

        // When
        final GroupCounts counts = graph.execute(new Builder()
                .first(new GetAllElements<>())
                .then(new CountGroups(limit))
                .build(), user);

        // Then
        int totalCount = (null != counts.getEntityGroups().get(TestGroups.ENTITY) ? counts.getEntityGroups().get(TestGroups.ENTITY) : 0);
        totalCount += (null != counts.getEdgeGroups().get(TestGroups.EDGE) ? counts.getEdgeGroups().get(TestGroups.EDGE) : 0);
        assertEquals(limit, totalCount);
    }
}
