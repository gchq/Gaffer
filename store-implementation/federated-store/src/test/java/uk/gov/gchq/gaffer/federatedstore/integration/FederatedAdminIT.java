package uk.gov.gchq.gaffer.federatedstore.integration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.PublicAccessPredefinedFederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FederatedAdminIT extends AbstractStoreIT {

    public static final User ADMIN_USER = new User("admin", Collections.EMPTY_SET, Sets.newHashSet("AdminAuth"));

    @Override
    protected Schema createSchema() {
        final Schema.Builder schemaBuilder = new Schema.Builder(createDefaultSchema());
        schemaBuilder.edges(Collections.EMPTY_MAP);
        schemaBuilder.entities(Collections.EMPTY_MAP);
        return schemaBuilder.build();
    }

    @Before
    public void setUp() throws Exception {
        graph.execute(new RemoveGraph.Builder()
                .graphId(PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_EDGES)
                .build(), user);
        graph.execute(new RemoveGraph.Builder()
                .graphId(PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_ENTITIES)
                .build(), user);
    }

    @Test
    public void shouldRemoveGraphForAdmin() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(StoreProperties.loadStoreProperties(StreamUtil.openStream(getClass(), "properties/singleUseMockAccStore.properties")))
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        graph.execute(new RemoveGraph.Builder()
                .graphId(graphA)
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertEquals(0, Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).size());

    }

    @Test
    public void shouldGetAllGraphIdsForAdmin() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(StoreProperties.loadStoreProperties(StreamUtil.openStream(getClass(), "properties/singleUseMockAccStore.properties")))
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Iterable<? extends String> adminGraphIds = graph.execute(new GetAllGraphIds.Builder()
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertTrue(Lists.newArrayList(adminGraphIds).contains(graphA));
    }
}
