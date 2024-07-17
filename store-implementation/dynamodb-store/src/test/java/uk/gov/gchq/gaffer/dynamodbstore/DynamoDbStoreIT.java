package uk.gov.gchq.gaffer.dynamodbstore;

import java.net.URI;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.StoreProperties;

import static org.assertj.core.api.Assertions.assertThat;

class DynamoDbStoreIT {

    @BeforeAll
    static void setUp() throws Exception {
        final String[] localArgs = {"-inMemory", "-port", "8000"};
        DynamoDBProxyServer server = ServerRunner.createServerFromCommandLineArgs(localArgs);
        server.start();
    }

    @Test
    void shouldCreateTableForGraph() {
        final String graphId = "graph1";
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .build())
                .storeProperties(this.getClass().getClassLoader().getResource("store.properties").getPath())
                .addSchemas(StreamUtil.openStreams(this.getClass(), "/schema"))
                .build();

        DynamoDbClient client = getClientFromProperties(graph.getStoreProperties());

        assertThat(DynamoDbStore.describeTable(client, graphId).tableName()).isEqualTo(graphId);
    }


    private DynamoDbClient getClientFromProperties(StoreProperties properties) {
        AwsCredentials credentials = AwsBasicCredentials.create(
            properties.get(DynamoDbStore.PROPERTY_CREDENTIAL_KEY),
            properties.get(DynamoDbStore.PROPERTY_CREDENTIAL_SECRET));
        // Return client using current config
        return DynamoDbClient.builder()
            .endpointOverride(URI.create(properties.get(DynamoDbStore.PROPERTY_URL)))
            .httpClient(UrlConnectionHttpClient.builder().build())
            .region(Region.of(properties.get(DynamoDbStore.PROPERTY_REGION)))
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();
    }



}
