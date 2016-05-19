/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.spark;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import gaffer.accumulostore.AccumuloProperties;
import gaffer.commonutil.StreamUtil;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.Properties;
import gaffer.data.elementdefinition.view.View;
import gaffer.graph.Graph;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.example.films.data.Certificate;
import gaffer.example.films.data.SampleData;
import gaffer.example.films.generator.DataGenerator;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.schema.Schema;
import gaffer.user.User;
import scala.Tuple2;

/**
 *
 */
public class GafferTableData {

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferTableData.class);

    protected Store store;
    protected MiniAccumuloCluster mini;
    protected InputStream propFile;
    protected AccumuloProperties prop;

    protected Set<Tuple2<Element, Properties>> entityExpectedUnrolledOutput = new HashSet<>();
    protected Set<Tuple2<Element, Properties>> entityExpectedOutput = new HashSet<>();
    protected Set<Tuple2<Element, Properties>> edgeExpectedUnrolledOutput = new HashSet<>();
    protected Set<Tuple2<Element, Properties>> edgeExpectedOutput = new HashSet<>();
    protected Set<Tuple2<Element, Properties>> expectedUnrolledOutput = new HashSet<>();
    protected Set<Tuple2<Element, Properties>> expectedOutput = new HashSet<>();

    /**
     * The user for the user doing the query.
     * Here we are setting the authorisation to include all certificates so the user will be able to see all the data.
     */
    private static final User USER = new User.Builder()
            .userId("user02")
            .dataAuth(Certificate.U.name())
            .dataAuth(Certificate.PG.name())
            .dataAuth(Certificate._12A.name())
            .dataAuth(Certificate._15.name())
            .dataAuth(Certificate._18.name())
            .build();

    public GafferTableData() {
        run();
    }

    public Store getStore() {
        return store;
    }
    /**
     * Method to open store.properties file and use to create Gaffer User and Table in Accumulo
     * ready to allow AccumuloStore object to be used to store Gaffer data in Accumulo instance.
     * 
     * @return Boolean indicating whether Gaffer User and Table have been created succesfully
     */
    private Boolean createGafferUserAndTable() {

        String instance = "gaffer";
        String zkServers = "localhost:2181";
        String localUser = "user01";
        String tableName = "table1";
        AuthenticationToken authLocalToken = new PasswordToken("password");

        String principal = "root";
        AuthenticationToken authToken = new PasswordToken("password");

        try {
            propFile = StreamUtil.openStream(getClass(), "/properties/accumulostore.properties", true);

            if(propFile != null) {
                prop = AccumuloProperties.loadStoreProperties(propFile);

                File tempDir = Files.createTempDir();
                tempDir.deleteOnExit();
                mini = new MiniAccumuloCluster(tempDir, "password");
                mini.start();
                
                instance = mini.getInstanceName();
                zkServers = mini.getZooKeepers();
                prop.set("accumulo.instance", instance);
                prop.set("accumulo.zookeepers", zkServers);

                localUser = prop.get("accumulo.user");
                tableName = prop.get("accumulo.table");
                authLocalToken = new PasswordToken(prop.get("accumulo.password"));
            }
        } catch (IOException ioe) {
            LOGGER.error("Exception opening properties file " + ioe.getMessage());
            return false;
        } catch (InterruptedException e) {
            LOGGER.error("Exception running MiniAccumuloCluster " + e.getMessage());
        }

        // Creates Zookeeper instance to allow connections into Accumulo
        ZooKeeperInstance inst = new ZooKeeperInstance(instance, zkServers);

        try {
            // Create Connection to Accumulo as principal user.
            Connector conn = inst.getConnector(principal, authToken);

            // Retrieve Set of current Accumulo users and if localUser exists skip creation.
            Set<String> users = conn.securityOperations().listLocalUsers();

            if(!users.contains(localUser)) {
                conn.securityOperations().createLocalUser(localUser,(PasswordToken) authLocalToken);
            }

            // Grant localUser permissions to create tables in Accumulo.
            conn.securityOperations().grantSystemPermission(localUser,SystemPermission.CREATE_TABLE);

            // Create Connection to Accumulo as localUser and Create Gaffer table if it doesn't exist.
            Connector localconn = inst.getConnector(localUser,authLocalToken);
            if (!localconn.tableOperations().exists(tableName)) {
                localconn.tableOperations().create(tableName);
            }

            // Create Array of authorisations to assign to Accumulo User
            String[] auths = {Certificate.U.name(),
                    Certificate.PG.name(),
                    Certificate._12A.name(),
                    Certificate._15.name(),
                    Certificate._18.name()};
            Authorizations listauth = new Authorizations(auths);
            conn.securityOperations().changeUserAuthorizations(localUser, listauth);

            return true;

        } catch (TableExistsException e) {
            LOGGER.error("Accumulo Table already exists " + e.getMessage());
            return true;
        } catch (AccumuloException e) {
            LOGGER.error("Exception writing to Accumulo " + e.getMessage());
            return true;
        } catch (AccumuloSecurityException e) {
            LOGGER.error("Security Exception writing to Accumulo " + e.getMessage());
            return true;
        }
    }

    public void run() {
        if(createGafferUserAndTable()) {
            Schema schema = Schema.fromJson(
                    StreamUtil.dataSchema(getClass()),
                    StreamUtil.dataTypes(getClass()),
                    StreamUtil.storeSchema(getClass()),
                    StreamUtil.storeTypes(getClass()));

            final String storeClass = prop.getStoreClass();
            if (null == storeClass) {
                throw new IllegalArgumentException(
                        "The Store class name was not found in the store properties for key: " + StoreProperties.STORE_PROPERTIES_CLASS);
            }

            try {
                store = Class.forName(storeClass).asSubclass(Store.class).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not create store of type: " + storeClass);
            }

            try {
                store.initialise(schema, prop);
            } catch (StoreException e) {
                throw new IllegalArgumentException("Could not initialise the store with provided arguments.", e);
            }

            // Setup graph
            final Graph graph = new Graph.Builder()
                    .storeProperties(prop)
                    .addSchema(StreamUtil.openStream(getClass(), "/schema/dataSchema.json", true))
                    .addSchema(StreamUtil.openStream(getClass(), "/schema/dataTypes.json", true))
                    .addSchema(StreamUtil.openStream(getClass(), "/schema/storeTypes.json", true))
                    .build();

            // Populate the graph with some example data
            // Create an operation chain. The output from the first operation is passed in as the input the second operation.
            // So the chain operation will generate elements from the domain objects then add these elements to the graph.
            final OperationChain<Void> populateChain = new OperationChain.Builder()
                    .first(new GenerateElements.Builder<>()
                            .objects(new SampleData().generate())
                            .generator(new DataGenerator())
                            .build())
                    .then(new AddElements.Builder()
                            .build())
                    .build();


            // Execute the operation chain on the graph
            try {
                graph.execute(populateChain, USER);

                GetEntitiesBySeed.Builder getEntities = new GetEntitiesBySeed.Builder()
                        .view(new View.Builder()
                                .entities(store.getSchema().getEntityGroups())
                                .build())
                        .addSeed(new EntitySeed("filmA"))
                        .addSeed(new EntitySeed("filmB"))
                        .addSeed(new EntitySeed("filmC"))
                        .addSeed(new EntitySeed("user01"))
                        .addSeed(new EntitySeed("user02"))
                        .addSeed(new EntitySeed("user03"));
                //.option(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, AUTH);

                final OperationChain<Iterable<Entity>> entityUnrolledChain = new OperationChain.Builder()
                        .first(getEntities.build())
                        .build();

                for(Entity e: graph.execute(entityUnrolledChain, USER)) {
                    entityExpectedUnrolledOutput.add(new Tuple2<Element, Properties>(e, e.getProperties()));
                }

                final OperationChain<Iterable<Entity>> entityChain = new OperationChain.Builder()
                        .first(getEntities
                                .summarise(true)
                                .build())
                        .build();

                for(Entity e: graph.execute(entityChain, USER)) {
                    entityExpectedOutput.add(new Tuple2<Element, Properties>(e, e.getProperties()));
                }

                GetRelatedEdges.Builder getEdges = new GetRelatedEdges.Builder()
                        .view(new View.Builder()
                                .edges(store.getSchema().getEdgeGroups())
                                .build())
                        .addSeed(new EntitySeed("filmA"))
                        .addSeed(new EntitySeed("filmB"))
                        .addSeed(new EntitySeed("filmC"));
                //.option(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, AUTH);

                final OperationChain<Iterable<Edge>> edgeUnrolledChain = new OperationChain.Builder()
                        .first(getEdges.build())
                        .build();

                for(Edge e: graph.execute(edgeUnrolledChain, USER)) {
                    edgeExpectedUnrolledOutput.add(new Tuple2<Element, Properties>(e, e.getProperties()));
                }

                final OperationChain<Iterable<Edge>> edgeChain = new OperationChain.Builder()
                        .first(getEdges
                                .summarise(true)
                                .build())
                        .build();

                for(Edge e: graph.execute(edgeChain, USER)) {
                    edgeExpectedOutput.add(new Tuple2<Element, Properties>(e, e.getProperties()));
                }
            } catch (OperationException e) {
                LOGGER.error(e.getMessage());
            }
            expectedUnrolledOutput.addAll(entityExpectedUnrolledOutput);
            expectedUnrolledOutput.addAll(edgeExpectedUnrolledOutput);
            expectedOutput.addAll(entityExpectedOutput);
            expectedOutput.addAll(edgeExpectedOutput);
        }

    }

    public void stopCluster() {
        if(mini != null)
            try {
                mini.stop();
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
            }
    }
}