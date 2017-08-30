package uk.gov.gchq.gaffer.traffic;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import java.io.IOException;
import java.io.InputStream;

public class SchemaIT {
    @Test
    public void shouldCreateGraphWithSchemaAndProperties() throws IOException {
        // Given
        final InputStream storeProps = StreamUtil.openStream(getClass(), "/mockaccumulo.properties");
        final InputStream[] schema = StreamUtil.schemas(ElementGroup.class);

        // When
        new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .storeProperties(storeProps)
                .addSchemas(schema)
                .build();

        // Then - no exceptions thrown
    }
}
