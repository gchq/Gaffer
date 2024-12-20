package uk.gov.gchq.gaffer.cassandra;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
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

public class CassandraStore  extends Store {

    public static final String KEYSPACE_NAME = "my_graphs";

    public CqlSession getClusterConnection() {
        return CqlSession.builder()
            .addContactPoint(new InetSocketAddress("localhost", 8090))
            .build();
    }

    public void createTableForGraph(final String graphId, final Schema schema) {
        Map<String, Object> replicationOptions = new HashMap<>();
            replicationOptions.put("class", "NetworkTopologyStrategy");
            replicationOptions.put("replication_factor", "3");
        CqlSession session = getClusterConnection();

        String createKeyspaceQuery = SchemaBuilder.createKeyspace(KEYSPACE_NAME)
            .ifNotExists()
            .withReplicationOptions(replicationOptions)
            .asCql();

        // Map the gaffer schema to the table
        // This can be adapted but generally should allow ID to be byte array
        // The direction plays a part in the key as encoded int: 1=entity, 2=source,
        // 3=dest, 4=undirected
        String createTableBuilder = SchemaBuilder.createTable(KEYSPACE_NAME, graphId)
            .ifNotExists()
            .withPartitionKey("id", DataTypes.BLOB)
            .withClusteringColumn("direction", DataTypes.TINYINT)
            .withColumn("group", DataTypes.TEXT)
            .withColumn("visibility", DataTypes.TEXT)
            .withColumn("properties", DataTypes.mapOf(DataTypes.TEXT, DataTypes.BLOB))
            .withColumn("timestamp", DataTypes.TIMESTAMP)
            .asCql();

        session.execute(createKeyspaceQuery);
        session.execute(createTableBuilder);
    }


    public void insertIntoTable(final String graphId, final Element element) throws SerialisationException {
        CqlSession session = getClusterConnection();

        InsertInto insertInto = QueryBuilder.insertInto(KEYSPACE_NAME, graphId);
        if (element instanceof Entity) {
            Entity entity = (Entity) element;
            // Build and add the entity
            SimpleStatement query = insertInto
                .value("id", QueryBuilder.literal(JSONSerialiser.serialise(entity.getVertex())))
                .value("direction", QueryBuilder.literal(1))
                .value("group", QueryBuilder.literal(entity.getGroup()))
                .value("visibility", QueryBuilder.literal("n/a"))
                .value("properties", QueryBuilder.literal(entity.getProperties()))
                .value("timestamp", QueryBuilder.currentTimestamp())
                .build();
            session.execute(query.getQuery());
        }
    }

    public Element getById(final String graphId, final Object id) throws SerialisationException {
        CqlSession session = getClusterConnection();

        SimpleStatement query = QueryBuilder.selectFrom(KEYSPACE_NAME, graphId)
            .all().whereColumn("id").isEqualTo(QueryBuilder.literal(JSONSerialiser.serialise(id)))
            .build();

        ResultSet result = session.execute(query.getQuery());

        if (!result.iterator().hasNext()) {
            return null;
        }

        Row row = result.one();

        return new Entity.Builder()
            .vertex(JSONSerialiser.deserialise(row.getByteBuffer("id").array(), Object.class))
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
