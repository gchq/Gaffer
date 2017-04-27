package uk.gov.gchq.gaffer.traffic;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.traffic.generator.RoadTrafficElementGenerator;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.io.InputStream;

public class GraphIT {
    @Test
    public void shouldBeAbleToAddAllSampleDataToGraph() throws IOException, OperationException {
        // Given
        final InputStream storeProps = StreamUtil.openStream(getClass(), "/mockaccumulo.properties", true);
        final InputStream[] schema = StreamUtil.schemas(ElementGroup.class);
        // When
        final Graph graph = new Graph.Builder()
                .storeProperties(storeProps)
                .addSchemas(schema)
                .build();

        final OperationChain<Void> populateChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .input(IOUtils.readLines(StreamUtil.openStream(GraphIT.class, "roadTrafficSampleData.csv")))
                        .generator(new RoadTrafficElementGenerator())
                        .build())
                .then(new AddElements.Builder()
                        .skipInvalidElements(false)
                        .build())
                .build();

        graph.execute(populateChain, new User());

        // Then - no exceptions thrown
    }
}
