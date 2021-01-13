package uk.gov.gchq.gaffer.federatedstore;

import com.clearspring.analytics.util.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.federatedstore.integration.FederatedViewsIT.BASIC_ENTITY;

/**
 * The FederatedStoreToFederatedStore Test works as follows:
 *                                           --------------------
 *      FederatedStore                      |   GAFFER REST API |
 *           -> Proxy Store --------------> |                   |
 *                                          |   FederatedStore  |
 *                                          |   -> MapStore     |
 *                                          --------------------
 */
public class FederatedStoreToFederatedStoreTest {

    private Graph federatedStoreGraph;
    private Graph restApiFederatedGraph;

    @BeforeEach
    public void setUpStores() throws OperationException {
        ProxyProperties proxyProperties = new ProxyProperties();
        proxyProperties.setStoreClass(SingleUseFederatedStore.class);

        restApiFederatedGraph = new Graph.Builder()
            .storeProperties(proxyProperties)
            .config(new GraphConfig("RestApiGraph"))
            .addSchema(new Schema())
            .build();

        federatedStoreGraph = new Graph.Builder()
            .config(new GraphConfig("federatedStoreGraph"))
            .storeProperties(new FederatedStoreProperties())
            .build();

        connectGraphs();
        addMapStore();
    }

    private void addMapStore() throws OperationException {
        restApiFederatedGraph.execute(new AddGraph.Builder()
            .storeProperties(new MapStoreProperties())
            .graphId("mapStore")
            .schema(Schema.fromJson(getClass().getResourceAsStream("/schema/basicEntitySchema.json")))
            .build(), new User());
    }

    private void connectGraphs() throws OperationException {
        federatedStoreGraph.execute(new AddGraph.Builder()
            .storeProperties(new ProxyProperties())
            .graphId("RestProxy")
            .schema(new Schema())
            .build(), new User());
    }

    @Test
    public void shouldBeAbleToSendViewedQueries() throws OperationException {
        // Given
        Entity entity = new Entity.Builder()
            .group(BASIC_ENTITY)
            .vertex("myVertex")
            .property("property1", 1)
            .build();

        restApiFederatedGraph.execute(new AddElements.Builder()
            .input(entity)
            .build(), new User());

        // When
        List<? extends Element> results = Lists.newArrayList(federatedStoreGraph.execute(new GetAllElements.Builder()
            .view(new View.Builder()
                .entity(BASIC_ENTITY)
                .build())
            .build(), new User()));

        // Then
        assertEquals(1, results.size());
        assertEquals(entity, results.get(0));
    }
}
