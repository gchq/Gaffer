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

package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractOperationsTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    protected static User USER = new User();
    protected Graph graph;
    protected List<ElementSeed> seedsList;
    protected View view;

    protected abstract Schema getSchema();

    protected abstract void setupSeeds();

    protected abstract void setupView();

    protected abstract void checkData(Graph graph, CloseableIterable<? extends Element> data) throws IOException, OperationException;

    protected abstract void checkGetSeededElementsData(CloseableIterable<? extends Element> data) throws IOException, OperationException;

    protected abstract void checkGetFilteredElementsData(CloseableIterable<? extends Element> data) throws IOException, OperationException;

    protected abstract void checkGetSeededAndFilteredElementsData(CloseableIterable<? extends Element> data) throws IOException, OperationException;

    protected Graph getGraph() throws IOException {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getSchema())
                .storeProperties(TestUtils.getParquetStoreProperties(testFolder))
                .build();
    }

    @Test
    public void getAllElementsTest() throws IOException, OperationException {
        final CloseableIterable<? extends Element> data = graph
                .execute(new GetAllElements.Builder().build(), USER);
        checkData(graph, data);
        data.close();
    }

    @Test
    public void getElementsTest() throws OperationException {
        final CloseableIterable<? extends Element> data = graph
                .execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).build(), USER);
        assertFalse(data.iterator().hasNext());
        data.close();
    }

    @Test
    public void getSeededElementsTest() throws IOException, OperationException {
        setupSeeds();
        final CloseableIterable<? extends Element> data = graph
                .execute(new GetElements.Builder().input(seedsList).build(), USER);
        checkGetSeededElementsData(data);
        data.close();
    }

    @Test
    public void getFilteredElementsTest() throws IOException, OperationException {
        setupView();
        final CloseableIterable<? extends Element> data = graph
                .execute(new GetAllElements.Builder().view(view).build(), USER);
        checkGetFilteredElementsData(data);
        data.close();
    }

    @Test
    public void getSeededAndFilteredElementsTest() throws IOException, OperationException {
        setupSeeds();
        setupView();
        final CloseableIterable<? extends Element> data = graph
                .execute(new GetElements.Builder().input(seedsList).view(view).build(), USER);
        checkGetSeededAndFilteredElementsData(data);
        data.close();
    }

    @Test
    public void getElementsWithPostAggregationFilterTest() throws OperationException {
        final View view = new View.Builder().edge(TestGroups.EDGE,
                new ViewElementDefinition.Builder()
                        .postAggregationFilter(
                                new ElementFilter.Builder()
                                        .select("double")
                                        .execute(
                                                new IsEqual(2.0))
                                        .build())
                        .build())
                .build();
        try {
            graph.execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).view(view).build(), USER);
            fail("IllegalArgumentException Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Operation chain"));
        } catch (final Exception ex) {
            fail("IllegalArgumentException expected");
        }
    }

    @Test
    public void getElementsWithPostTransformFilterTest() throws OperationException {
        final View view = new View.Builder().edge(TestGroups.EDGE,
                new ViewElementDefinition.Builder()
                        .postTransformFilter(
                                new ElementFilter.Builder()
                                        .select("double")
                                        .execute(
                                                new IsEqual(2.0))
                                        .build())
                        .build())
                .build();
        try {
            graph.execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).view(view).build(), USER);
            fail("IllegalArgumentException Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Operation chain"));
        } catch (final Exception ex) {
            fail("IllegalArgumentException expected");
        }
    }
}
