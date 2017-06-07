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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public abstract class AbstractOperationsTest {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractOperationsTest.class);
    static User USER = new User();
    Graph graph;
    ArrayList<ElementSeed> seedsList;
    View view;

    abstract void setupSeeds();
    abstract void setupView();
    abstract void checkData(CloseableIterable<? extends Element> data);
    abstract void checkGetSeededElementsData(CloseableIterable<? extends Element> data);
    abstract void checkGetFilteredElementsData(CloseableIterable<? extends Element> data);
    abstract void checkGetSeededAndFilteredElementsData(CloseableIterable<? extends Element> data);

    @AfterClass
    public static void cleanUpData() throws IOException {
        LOGGER.info("Cleaning up the data");
        final FileSystem fs = FileSystem.get(new Configuration());
        final ParquetStoreProperties props = (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                StreamUtil.storeProps(AbstractOperationsTest.class));
        deleteFolder(props.getDataDir(), fs);
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

    void checkEdge(final Edge edge, final String expectedGroup, final Object expectedSource, final Object expectedDestination,
                   final boolean expectedIsDirected, final Byte p1, final Double p2, final Float p3, final Long p4,
                   final Long p5, final Short p6, final Date p7, final Integer count) {
        assertEquals(expectedGroup, edge.getGroup());
        assertEquals(expectedSource, edge.getSource());
        assertEquals(expectedDestination, edge.getDestination());
        assertEquals(expectedIsDirected, edge.isDirected());
        checkProperties(edge.getProperties(), p1, p2, p3, p4, p5, p6, p7, count);
    }

    void checkEntity(final Entity entity, final String expectedGroup, final Object expectedVertex, final Byte p1,
                     final Double p2, final Float p3, final Long p4, final Long p5, final Short p6,
                     final Date p7, final Integer count) {
        assertEquals(expectedGroup, entity.getGroup());
        assertEquals(expectedVertex, entity.getVertex());
        checkProperties(entity.getProperties(), p1, p2, p3, p4, p5, p6, p7, count);
    }

    private void checkProperties(final Properties properties, final Byte p1, final Double p2, final Float p3, final Long p4,
                                 final Long p5, final Short p6, final Date p7, final Integer count) {
        assertEquals(p1, properties.get("property1"));
        if (p2 == null) {
            assertEquals(null, properties.get("property2"));
        } else {
            assertEquals(p2, (Double) properties.get("property2"), 0.01);
        }
        assertEquals(p3, properties.get("property3"));
        if (p4 == null) {
            assertEquals(null, properties.get("property4"));
        } else {
            assertEquals((long) p4, ((HyperLogLogPlus) properties.get("property4")).cardinality());
        }

        assertEquals(p5, properties.get("property5"));
        assertEquals(p6, properties.get("property6"));
        if (p7 != null) {
            assertTrue((p7.getTime() + 20) + " should be greater than " + ((Date) properties.get("property7")).getTime(), p7.getTime() + 20 > ((Date) properties.get("property7")).getTime());
            assertTrue((p7.getTime() - 20) + " should be less than " + ((Date) properties.get("property7")).getTime(), p7.getTime() - 20 < ((Date) properties.get("property7")).getTime());
        }
        if (p4 == null) {
            assertEquals(null, properties.get("property8"));
        } else {
            assertEquals((long) p4, ((HyperLogLogPlus) properties.get("property8")).cardinality());
        }
        assertEquals(count, properties.get("count"));
    }

    @Test
    public void getAllElementsTest() throws OperationException {
        LOGGER.info("Starting getAllElementsTest");
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetAllElements.Builder().build(), USER);
        checkData(data);
        data.close();
    }

    @Test
    public void getElementsTest() throws OperationException {
        LOGGER.info("Starting getElementsTest");
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetElements.Builder().build(), USER);
        assertFalse(data.iterator().hasNext());
        data.close();
    }

    @Test
    public void getSeededElementsTest() throws OperationException {
        LOGGER.info("Starting getSeededElementsTest");
        setupSeeds();
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetElements.Builder().input(this.seedsList).build(), USER);
        checkGetSeededElementsData(data);
        data.close();
    }

    @Test
    public void getFilteredElementsTest() throws OperationException {
        LOGGER.info("Starting getFilteredElementsTest");
        setupView();
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetAllElements.Builder().view(this.view).build(), USER);
        checkGetFilteredElementsData(data);
        data.close();
    }

    @Test
    public void getSeededAndFilteredElementsTest() throws OperationException {
        LOGGER.info("Starting getSeededAndFilteredElementsTest");
        setupSeeds();
        setupView();
        final CloseableIterable<? extends Element> data = this.graph.execute(new GetElements.Builder().input(this.seedsList).view(this.view).build(), USER);
        checkGetSeededAndFilteredElementsData(data);
        data.close();
    }

    @Test
    public void getElementsWithPostAggregationFilterTest() throws OperationException {
        LOGGER.info("Starting getElementsWithInvalidViewTest");
        final View view = new View.Builder().edge("BasicEdge",
                new ViewElementDefinition.Builder()
                        .postAggregationFilter(
                                new ElementFilter.Builder()
                                        .select("property2")
                                        .execute(
                                                new IsEqual(2.0))
                                        .build())
                        .build())
                .build();
        try {
            this.graph.execute(new GetElements.Builder().view(view).build(), USER);
            fail();
        } catch (OperationException e) {
            assertEquals("The ParquetStore does not currently support post aggregation filters.", e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void getElementsWithPostTransformFilterTest() throws OperationException {
        LOGGER.info("Starting getElementsWithInvalidViewTest");
        final View view = new View.Builder().edge("BasicEdge",
                new ViewElementDefinition.Builder()
                        .postTransformFilter(
                                new ElementFilter.Builder()
                                        .select("property2")
                                        .execute(
                                                new IsEqual(2.0))
                                        .build())
                        .build())
                .build();
        try {
            this.graph.execute(new GetElements.Builder().view(view).build(), USER);
            fail();
        } catch (OperationException e) {
            assertEquals("The ParquetStore does not currently support post transformation filters.", e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }
}
