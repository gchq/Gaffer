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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;
import java.io.IOException;

public class ExportToOtherGraphExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new ExportToOtherGraphExample().run();
    }

    public ExportToOtherGraphExample() {
        super(ExportToOtherGraph.class);
    }

    @Override
    public void runExamples() {
        simpleExport();
        simpleExportUsingGraphLibrary();
        exportUsingParentIdsAndGraphLibrary();
        cleanUp();
    }

    public Iterable<?> simpleExport() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<? extends Element>> opChain;
        final AccumuloProperties storeProperties = new AccumuloProperties();
        final Schema schema = new Schema.Builder().build();
        try {
            storeProperties.getProperties().load(StreamUtil.openStream(getClass(), "othermockaccumulostore.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        opChain = new OperationChain.Builder()
                .first(new GetElements())
                .then(new ExportToOtherGraph.Builder<CloseableIterable<? extends Element>>()
                        .graphId("graphId")
                        .storeProperties(storeProperties)
                        .schema(schema)
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }

    public Iterable<?> simpleExportUsingGraphLibrary() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<? extends Element>> opChain;

        final AccumuloProperties storeProperties = new AccumuloProperties();
        storeProperties.setId("storePropertiesId");

        final Schema schema = new Schema.Builder().id("schemaId").build();

        FileGraphLibrary graphLibrary = new FileGraphLibrary("doc/src/main/resources/ExportToOtherGraphGraphLibrary");
        graphLibrary.addOrUpdate("graphId1", schema, storeProperties);

        try {
            storeProperties.getProperties().load(StreamUtil.openStream(getClass(), "othermockaccumulostore.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        opChain = new OperationChain.Builder()
                .first(new GetElements())
                .then(new ExportToOtherGraph.Builder<CloseableIterable<? extends Element>>()
                        .graphId("graphId1")
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }

    public Iterable<?> exportUsingParentIdsAndGraphLibrary() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<? extends Element>> opChain;

        final AccumuloProperties storeProperties = new AccumuloProperties();
        storeProperties.setId("storePropertiesId");
        try {
            storeProperties.getProperties().load(StreamUtil.openStream(getClass(), "othermockaccumulostore.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final AccumuloProperties storeProperties1 = new AccumuloProperties();
        storeProperties1.setId("storePropertiesId1");
        try {
            storeProperties1.getProperties().load(StreamUtil.openStream(getClass(), "othermockaccumulostore.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final Schema schema = new Schema.Builder().id("schemaId").build();
        final Schema schema1 = new Schema.Builder().id("schemaId1").build();

        FileGraphLibrary graphLibrary = new FileGraphLibrary("doc/src/main/resources/ExportToOtherGraphGraphLibrary");

        getGraph().setLibrary(graphLibrary);
        getGraph().getLibrary().addOrUpdate("graphId2", schema, storeProperties);
        getGraph().getLibrary().addProperties(storeProperties1.getId(), storeProperties1);
        getGraph().getLibrary().addSchema(schema1.getId(), schema1);

        try {
            storeProperties.getProperties().load(StreamUtil.openStream(getClass(), "othermockaccumulostore.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        opChain = new OperationChain.Builder()
                .first(new GetElements())
                .then(new ExportToOtherGraph.Builder<CloseableIterable<? extends Element>>()
                        .graphId("graphId3")
                        .parentStorePropertiesId(storeProperties1.getId())
                        .parentSchemaId(schema1.getId())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }

    private void cleanUp() {
        try {
            if (new File("doc/src/main/resources/ExportToOtherGraphGraphLibrary").exists()) {
                FileUtils.forceDelete(new File("doc/src/main/resources/ExportToOtherGraphGraphLibrary"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
