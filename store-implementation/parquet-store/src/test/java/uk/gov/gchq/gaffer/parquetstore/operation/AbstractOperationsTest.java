/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractOperationsTest {
    static User USER = new User();
    Graph graph;
    List<ElementSeed> seedsList;
    View view;

    abstract void setupSeeds();

    abstract void setupView();

    abstract void checkData(CloseableIterable<? extends Element> data);

    abstract void checkGetSeededElementsData(CloseableIterable<? extends Element> data);

    abstract void checkGetFilteredElementsData(CloseableIterable<? extends Element> data);

    abstract void checkGetSeededAndFilteredElementsData(CloseableIterable<? extends Element> data);

    @AfterClass
    public static void cleanUpData() throws IOException {
        try (final FileSystem fs = FileSystem.get(new Configuration())) {
            final ParquetStoreProperties props = TestUtils.getParquetStoreProperties();
            deleteFolder(props.getDataDir(), fs);
        }
    }

    private static void deleteFolder(final String path, final FileSystem fs) throws IOException {
        Path dataDir = new Path(path);
        if (fs.exists(dataDir)) {
            fs.delete(dataDir, true);
            while (fs.listStatus(dataDir.getParent()).length == 0) {
                dataDir = dataDir.getParent();
                fs.delete(dataDir, true);
            }
        }
    }

    @Test
    public void getAllElementsTest() throws OperationException {
        final CloseableIterable<? extends Element> data = graph.execute(new GetAllElements.Builder().build(), USER);
        checkData(data);
        data.close();
    }

    @Test
    public void getElementsTest() throws OperationException {
        final CloseableIterable<? extends Element> data = graph.execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).build(), USER);
        assertFalse(data.iterator().hasNext());
        data.close();
    }

    @Test
    public void getSeededElementsTest() throws OperationException {
        setupSeeds();
        final CloseableIterable<? extends Element> data = graph.execute(new GetElements.Builder().input(seedsList).build(), USER);
        checkGetSeededElementsData(data);
        data.close();
    }

    @Test
    public void getFilteredElementsTest() throws OperationException {
        setupView();
        final CloseableIterable<? extends Element> data = graph.execute(new GetAllElements.Builder().view(view).build(), USER);
        checkGetFilteredElementsData(data);
        data.close();
    }

    @Test
    public void getSeededAndFilteredElementsTest() throws OperationException {
        setupSeeds();
        setupView();
        final CloseableIterable<? extends Element> data = graph.execute(new GetElements.Builder().input(seedsList).view(view).build(), USER);
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
