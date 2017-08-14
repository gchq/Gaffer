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
package uk.gov.gchq.gaffer.doc.operation;

import org.apache.commons.io.FileUtils;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.proxystore.ProxyStore;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;
import java.io.File;
import java.io.IOException;

public class ExportToOtherGraphExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new ExportToOtherGraphExample().run();
    }

    public ExportToOtherGraphExample() {
        super(ExportToOtherGraph.class, "These export examples export all edges in the example graph to another Gaffer instance. \n\n" +
                "To add this operation to your Gaffer graph you will need to include the ExportToOtherGraphOperationDeclarations.json in your store properties, i.e. set this property: " +
                "gaffer.store.operation.declarations=ExportToOtherGraphOperationDeclarations.json\n");
    }

    @Override
    public void runExamples() {
        simpleExport();
        simpleExportWithCustomGraph();
        simpleToOtherGafferRestApi();
        simpleExportUsingGraphFromGraphLibrary();
        exportToNewGraphBasedOnConfigFromGraphLibrary();
        cleanUp();
    }

    public void simpleExport() {
        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends Element>> opChain =
                new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .edge("edge")
                                        .build())
                                .build())
                        .then(new ExportToOtherGraph.Builder()
                                .graphId("newGraphId")
                                .build())
                        .build();
        // ---------------------------------------------------------

        showExample(opChain, "This example will export all Edges with group 'edge' to another Gaffer graph with new ID 'newGraphId'. " +
                "The new graph will have the same schema and same store properties as the current graph. " +
                "In this case it will just create another table in accumulo called 'newGraphId'.");
    }

    public void simpleExportWithCustomGraph() {
        // ---------------------------------------------------------
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "operation/schema"));
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(getClass(), "othermockaccumulostore.properties"));
        final OperationChain<Iterable<? extends Element>> opChain =
                new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .edge("edge")
                                        .build())
                                .build())
                        .then(new ExportToOtherGraph.Builder()
                                .graphId("newGraphId")
                                .schema(schema)
                                .storeProperties(storeProperties)
                                .build())
                        .build();
        // ---------------------------------------------------------

        showExample(opChain, "This example will export all Edges with group 'edge' to another Gaffer graph with new ID 'newGraphId'. " +
                "The new graph will have the custom provided schema (note it must contain the same Edge group 'edge' otherwise the exported edges will be invalid') and custom store properties. " +
                "The store properties could be any store properties e.g. Accumulo, HBase, Map, Proxy store properties.");
    }

    public void simpleToOtherGafferRestApi() {
        // ---------------------------------------------------------
        final ProxyProperties proxyProperties = new ProxyProperties();
        proxyProperties.setStoreClass(ProxyStore.class);
        proxyProperties.setStorePropertiesClass(ProxyProperties.class);
        proxyProperties.setGafferHost("localhost");
        proxyProperties.setGafferPort(8081);
        proxyProperties.setGafferContextRoot("/rest/v1");

        final OperationChain<Iterable<? extends Element>> opChain =
                new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .edge("edge")
                                        .build())
                                .build())
                        .then(new ExportToOtherGraph.Builder()
                                .graphId("otherGafferRestApiGraphId")
                                .storeProperties(proxyProperties)
                                .build())
                        .build();
        // ---------------------------------------------------------

        showExample(opChain, "This example will export all Edges with group 'edge' to another Gaffer REST API." +
                "To export to another Gaffer REST API, we go via a Gaffer Proxy Store. We just need to tell the proxy store the host, port and context root of the REST API." +
                "Note that you will need to include the proxy-store module as a maven dependency to do this.");
    }

    public void simpleExportUsingGraphFromGraphLibrary() {
        // ---------------------------------------------------------
        // Setup the graphLibrary with an export graph
        final FileGraphLibrary graphLibrary = new FileGraphLibrary("target/graphLibrary");

        final AccumuloProperties exportStoreProperties = new AccumuloProperties();
        exportStoreProperties.setId("exportStorePropertiesId");
        // set other store property config here.

        final Schema exportSchema = new Schema.Builder()
                .id("exportSchemaId")
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source("int")
                        .destination("int")
                        .directed("true")
                        .property("count", "int")
                        .aggregate(false)
                        .build())
                .type("int", Integer.class)
                .type("true", new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .validateFunctions(new IsTrue())
                        .build())
                .build();

        graphLibrary.addOrUpdate("exportGraphId", exportSchema, exportStoreProperties);

        final Graph graph = new Graph.Builder()
                .config(StreamUtil.openStream(getClass(), "graphConfigWithLibrary.json"))
                .addSchemas(StreamUtil.openStreams(getClass(), "operation/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();

        final OperationChain<Iterable<? extends Element>> opChain =
                new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .edge("edge")
                                        .build())
                                .build())
                        .then(new ExportToOtherGraph.Builder()
                                .graphId("exportGraphId")
                                .build())
                        .build();
        // ---------------------------------------------------------

        showExample(opChain, "This example will export all Edges with group 'edge' to another existing graph 'exportGraphId' using a GraphLibrary." +
                "We demonstrate here that if we use a GraphLibrary, we can register a graph ID and reference it from the export operation. " +
                "This means the user does not have to proxy all the schema and store properties when they configure the export operation, they can just provide the ID.");
    }

    public void exportToNewGraphBasedOnConfigFromGraphLibrary() {
        // ---------------------------------------------------------
        // Setup the graphLibrary with a schema and store properties for exporting
        final FileGraphLibrary graphLibrary = new FileGraphLibrary("target/graphLibrary");

        final AccumuloProperties exportStoreProperties = new AccumuloProperties();
        exportStoreProperties.setId("exportStorePropertiesId");
        // set other store property config here.
        graphLibrary.addProperties("exportStorePropertiesId", exportStoreProperties);

        final Schema exportSchema = new Schema.Builder()
                .id("exportSchemaId")
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source("int")
                        .destination("int")
                        .directed("true")
                        .property("count", "int")
                        .aggregate(false)
                        .build())
                .type("int", Integer.class)
                .type("true", new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .validateFunctions(new IsTrue())
                        .build())
                .build();
        graphLibrary.addSchema("exportSchemaId", exportSchema);

        final Graph graph = new Graph.Builder()
                .config(StreamUtil.openStream(getClass(), "graphConfigWithLibrary.json"))
                .addSchemas(StreamUtil.openStreams(getClass(), "operation/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();

        final OperationChain<Iterable<? extends Element>> opChain =
                new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .edge("edge")
                                        .build())
                                .build())
                        .then(new ExportToOtherGraph.Builder()
                                .graphId("newGraphId")
                                .parentSchemaIds("exportSchemaId")
                                .parentStorePropertiesId("exportStorePropertiesId")
                                .build())
                        .build();
        // ---------------------------------------------------------

        showExample(opChain, "Similar to the previous example, this example will export all Edges with group 'edge' to another graph using a GraphLibrary. " +
                "But in this example we show that you can export to a new graph with id newGraphId by chosing any combination of schema and store properties registered in the GraphLibrary. " +
                "This is useful as a system administrator could register various different store properties, of different Accumulo/HBase clusters and a user could them just select which one to use by referring to the relevant store properties ID.");
    }

    private void cleanUp() {
        try {
            if (new File("target/ExportToOtherGraphGraphLibrary").exists()) {
                FileUtils.forceDelete(new File("target/ExportToOtherGraphGraphLibrary"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
