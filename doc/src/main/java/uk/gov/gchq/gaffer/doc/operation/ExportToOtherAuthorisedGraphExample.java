/*
 * Copyright 2017 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherAuthorisedGraph;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

public class ExportToOtherAuthorisedGraphExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new ExportToOtherAuthorisedGraphExample().run();
    }

    public ExportToOtherAuthorisedGraphExample() {
        super(ExportToOtherAuthorisedGraph.class, "These export examples export all edges in the example graph to another Gaffer instance using Operation Auths against the user. \n\n" +
                "To add this operation to your Gaffer graph you will need to write a new version of ExportToOtherAuthorisedGraphOperationDeclarations.json containing the user auths" +
                ", and then set this property: gaffer.store.operation.declarations=ExportToOtherAuthorisedGraphOperationDeclarations.json\n");
    }

    @Override
    public void runExamples() {
        simpleExport();
        simpleExportUsingParentIds();
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
                        .then(new ExportToOtherAuthorisedGraph.Builder()
                                .graphId("newGraphId")
                                .build())
                        .build();
        // ---------------------------------------------------------

        showExample(opChain, "This example will export all Edges with group 'edge' to another Gaffer graph with new ID 'newGraphId'. " +
                "The new graph will have the same schema and same store properties as the current graph. " +
                "In this case it will just create another table in accumulo called 'newGraphId'. " +
                "It will also compare the running users Operation authorisations to the authorisations supplied to the exporter. " +
                "These will be specified in ExportToOtherAuthorisedGraphOperationDeclarations.json.");
    }

    public void simpleExportUsingParentIds() {

        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends Element>> opChain =
                new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .edge("edge")
                                        .build())
                                .build())
                        .then(new ExportToOtherAuthorisedGraph.Builder()
                                .graphId("newGraphId")
                                .parentStorePropertiesId("storePropsId1")
                                .parentSchemaIds("schemaId1")
                                .build())
                        .build();
        // ---------------------------------------------------------

        showExample(opChain, "This example will export all Edges with group 'edge' to another Gaffer graph with new ID 'newGraphId'. " +
                "The new graph will have a parent Schema and Store Properties within the graph library specifed by the ID's. " +
                "It will also compare the running users Operation authorisations to the authorisations supplied to the exporter. " +
                "These will be specified in ExportToOtherAuthorisedGraphOperationDeclarations.json.");
    }
}
