/*
 * Copyright 2017-2018. Crown Copyright
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
 * limitations under the License
 */

package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AbstractSparkOperationsTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    static User USER = new User.Builder().dataAuth("A").build();

    protected abstract void checkGetDataFrameOfElements(Dataset<Row> data, boolean withVisibilities);

    protected abstract Graph genData(final boolean withVisibilities) throws OperationException, StoreException, IOException;

    protected abstract Schema getSchema();

    protected abstract JavaRDD<Element> getElements(final JavaSparkContext spark, final boolean withVisibilities);

    protected static Graph getGraph(final Schema schema, final ParquetStoreProperties properties, final String graphID) throws StoreException {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphID)
                        .build())
                .addSchema(schema)
                .storeProperties(properties)
                .build();
    }

    @Test
    public void getDataFrameOfElementsTest() throws IOException, OperationException, StoreException {
        final Graph graph = genData(false);
        final Dataset<Row> data = graph.execute(new GetDataFrameOfElements.Builder()
                .build(), USER);
        checkGetDataFrameOfElements(data, false);
    }

    @Test
    public void getDataFrameOfElementsWithViewTest() throws IOException, OperationException, StoreException {
        final Graph graph = genData(false);
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY,
                        new ViewElementDefinition.Builder().preAggregationFilter(
                                new ElementFilter.Builder().select("double").execute(new IsEqual(0.2)).build()
                        ).build())
                .build();
        try {
            graph.execute(new GetDataFrameOfElements.Builder()
                    .view(view).build(), USER);
            fail();
        } catch (final OperationException e) {
            assertEquals("Views are not supported by this operation yet", e.getMessage());
        } catch (final Exception e) {
            fail();
        }
    }

    @Test
    public void getDataFrameOfElementsWithVisibilitiesTest() throws OperationException, StoreException, IOException {
        final Graph graph = genData(true);
        final Dataset<Row> data = graph.execute(new GetDataFrameOfElements.Builder()
                .build(), USER);
        checkGetDataFrameOfElements(data, true);
    }
}
