package uk.gov.gchq.gaffer.scylladbstore;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class ScyllaDbStore  extends Store {

    public static final String KEYSPACE_NAME = "my_graphs";

    public Session getClusterConnection() {
        Cluster cluster = Cluster.builder()
            .addContactPointsWithPorts(new InetSocketAddress("localhost", 8090))
            .withAuthProvider(new PlainTextAuthProvider("scylla", "awesome-password"))
            .build();

        return cluster.connect();
    }

    public void createTableForGraph(final String graphId, final Schema schema) {
        Map<String, Object> replicationOptions = new HashMap<>();
            replicationOptions.put("class", "NetworkTopologyStrategy");
            replicationOptions.put("replication_factor", "3");
        Session session = getClusterConnection();

        String createKeyspaceQuery = SchemaBuilder.createKeyspace(KEYSPACE_NAME)
                .ifNotExists()
                .with()
                .replication(replicationOptions)
                .durableWrites(true)
                .getQueryString();

        Create createTableBuilder = SchemaBuilder.createTable(KEYSPACE_NAME, graphId)
                .ifNotExists();

        // Map the gaffer schema to the table
        // This can be adapted but generally should allow ID to be byte array
        // The direction plays a part in the key as encoded int: 1=entity, 2=source, 3=dest, 4=undirected
        // Family is for clustering on edge or entity: 1=entity, 2=edge
        createTableBuilder
            .addPartitionKey("id", DataType.blob())
            .addPartitionKey("direction", DataType.tinyint())
            .addClusteringColumn("family", DataType.tinyint())
            .addColumn("group", DataType.text())
            .addColumn("visibility", DataType.text())
            .addColumn("properties", DataType.map(DataType.text(), DataType.blob()))
            .addColumn("timestamp", DataType.timestamp());

        session.execute(createKeyspaceQuery);
        session.execute(createTableBuilder.getQueryString());
    }


    public void insertIntoTable(final String graphId, final Element element) throws SerialisationException {
        Session session = getClusterConnection();

        InsertInto insertInto = QueryBuilder.insertInto(KEYSPACE_NAME, graphId);
        if (element instanceof Entity) {
            Entity entity = (Entity) element;
            // Build and add the entity
            SimpleStatement query = insertInto
                .value("id", QueryBuilder.literal(JSONSerialiser.serialise(entity.getVertex())))
                .value("direction", QueryBuilder.literal(1))
                .value("family", QueryBuilder.literal(1))
                .value("group", QueryBuilder.literal(entity.getGroup()))
                .value("visibility", QueryBuilder.literal("n/a"))
                .value("properties", QueryBuilder.literal(entity.getProperties()))
                .value("timestamp", QueryBuilder.currentTimestamp())
                .build();
            session.execute(query.getQuery());
        }
    }

    public Element getById(final String graphId, final Object id) throws SerialisationException {
        Session session = getClusterConnection();

        SimpleStatement query = QueryBuilder.selectFrom(KEYSPACE_NAME, graphId)
            .all().whereColumn("id").isEqualTo(QueryBuilder.literal(JSONSerialiser.serialise(id)))
            .build();

        ResultSet result = session.execute(query.getQuery());

        if (result.isExhausted()) {
            return null;
        }

        Row row = result.one();

        return new Entity.Builder()
            .vertex(JSONSerialiser.deserialise(row.getBytes("id").array(), Object.class))
            .group(row.getString("group"))
            .properties(row.getMap("properties", String.class, Object.class))
            .build();
    }


    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties)
            throws StoreException {
        super.initialise(graphId, schema, properties);

    }

    @Override
    protected void addAdditionalOperationHandlers() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addAdditionalOperationHandlers'");
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getGetElementsHandler'");
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getGetAllElementsHandler'");
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAdjacentIdsHandler'");
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAddElementsHandler'");
    }

    @Override
    protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDeleteElementsHandler'");
    }

    @Override
    protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDeleteAllDataHandler'");
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getGetTraitsHandler'");
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRequiredParentSerialiserClass'");
    }

}
