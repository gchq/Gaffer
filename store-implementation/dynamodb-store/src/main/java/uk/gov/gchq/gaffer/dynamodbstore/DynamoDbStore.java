package uk.gov.gchq.gaffer.dynamodbstore;

import java.net.URI;
import java.util.Set;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class DynamoDbStore extends Store {

    public static final String PROPERTY_URL = "dynamodb.url";
    public static final String PROPERTY_REGION = "dynamodb.region";
    public static final String PROPERTY_CREDENTIAL_KEY = "dynamodb.credential.key";
    public static final String PROPERTY_CREDENTIAL_SECRET = "dynamodb.credential.key";

    public static final String GRAPH_TABLE_PRIMARY_KEY = "gafferGraph";

    public DynamoDbClient getClientConnection() {
        AwsCredentials credentials = AwsBasicCredentials.create(
            this.getProperties().get(PROPERTY_CREDENTIAL_KEY),
            this.getProperties().get(PROPERTY_CREDENTIAL_SECRET));
        // Return client using current config
        return DynamoDbClient.builder()
            .endpointOverride(URI.create(this.getProperties().get(PROPERTY_URL)))
            .httpClient(UrlConnectionHttpClient.builder().build())
            .region(Region.of(this.getProperties().get(PROPERTY_REGION)))
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();
    }


    public void ensureTableExists() throws StoreException {
        DynamoDbClient client = getClientConnection();
        try {
            // Do nothing if table exists
            describeTable(client, getGraphId());
        } catch (ResourceNotFoundException e) {
            // Table does not exist yet
            createTableForGraph(client, getGraphId());
        }
    }

    public static void createTableForGraph(DynamoDbClient client, String tableName) throws StoreException {
        DynamoDbWaiter dbWaiter = client.waiter();

        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(GRAPH_TABLE_PRIMARY_KEY)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName(GRAPH_TABLE_PRIMARY_KEY)
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(pt -> pt.readCapacityUnits(10L).writeCapacityUnits(10L)
                        .build())
                .tableName(tableName)
                .build();

        try {
            client.createTable(request);
            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();

            // Wait until the Amazon DynamoDB table is created
            WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
            if (!waiterResponse.matched().response().isPresent()) {
                throw new StoreException("Missing response from table creation request");
            }

        } catch (DynamoDbException e) {
            throw new StoreException("Failed to create new table for store", e);
        }
    }

    public static TableDescription describeTable(DynamoDbClient client, String tableName) {
        DescribeTableRequest request = DescribeTableRequest.builder()
            .tableName(tableName)
            .build();

        return client.describeTable(request).table();
    }


    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties)
            throws StoreException {
        super.initialise(graphId, schema, properties);
        ensureTableExists();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        ///
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
        return null;
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        return null;
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return ToBytesSerialiser.class;
    }

}
