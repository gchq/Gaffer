package uk.gov.gchq.gaffer.integration.impl.loader;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.StoreIT;
import uk.gov.gchq.gaffer.integration.extensions.LoaderTestCase;
import uk.gov.gchq.gaffer.integration.extensions.LoaderTestContextProvider;
import uk.gov.gchq.gaffer.integration.factory.MapStoreGraphFactory;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(LoaderTestContextProvider.class)
public class AddElementsLoaderIT extends AbstractLoaderIT implements StoreIT {

    @Autowired
    private GraphFactory graphFactory;

    private void resetRemoteGraph(final Schema schema) {
        if (graphFactory instanceof MapStoreGraphFactory) {
            ((MapStoreGraphFactory) graphFactory).reset(schema);
        } else {
            throw new RuntimeException("Expected the MapStoreGraph Factory to be injected");
        }
    }

    @Override
    protected void beforeEveryTest(final LoaderTestCase testCase) throws Exception{
        resetRemoteGraph(testCase.getSchemaSetup().getTestSchema().getSchema());
        super.beforeEveryTest(testCase);
    }

    @Override
    public void addElements(final Graph graph, final Iterable<? extends Element> input) throws OperationException {
        graph.execute(new AddElements.Builder()
            .input(input)
            .build(), new User());
    }

    //////////////////////////////////////////////////////////////////
    //                  Add Elements error handling                 //
    //////////////////////////////////////////////////////////////////
    @LoaderTest(excludeStores = { ParquetStore.class, FederatedStore.class}) // todo find out why these fail
    public void shouldThrowExceptionWithUsefulMessageWhenInvalidElementsAdded(final LoaderTestCase testCase) throws OperationException {
        // Given
        resetRemoteGraph(testCase.getSchemaSetup().getTestSchema().getSchema());
        Graph graph = testCase.getGraph();

        // When
        final AddElements addElements = new AddElements.Builder()
            .input(new Edge("UnknownGroup", "source", "dest", true))
            .build();

        // Then
        Exception e = assertThrows(Exception.class, () -> graph.execute(addElements, new User()));

        String msg = e.getMessage();
        if (!msg.contains("Element of type Entity") && null != e.getCause()) {
            msg = e.getCause().getMessage();
        }
        assertTrue(msg.contains("UnknownGroup"), "Message was: " + msg);
    }

    @LoaderTest
    public void shouldNotThrowExceptionWhenInvalidElementsAddedWithSkipInvalidSetToTrue(final LoaderTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getGraph();

        final AddElements addElements = new AddElements.Builder()
            .input(new Edge("Unknown group", "source", "dest", true))
            .skipInvalidElements(true)
            .build();

        // When
        graph.execute(addElements, new User());

        // Then - no exceptions
    }

    @LoaderTest
    public void shouldNotThrowExceptionWhenInvalidElementsAddedWithValidateSetToFalse(final LoaderTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getGraph();
        final AddElements addElements = new AddElements.Builder()
            .input(new Edge("Unknown group", "source", "dest", true))
            .validate(false)
            .build();

        // When
        graph.execute(addElements, new User());

        // Then - no exceptions
    }
}
