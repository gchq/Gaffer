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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.data.DataGen;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class LongVertexOperationsTest extends AbstractOperationsTest {
    
    @BeforeClass
    public static void genData() throws OperationException {
        Logger.getRootLogger().setLevel(Level.INFO);
        getGraph().execute(new AddElements.Builder().input(getElements()).build(), USER);
    }

    @Before
    public void setup() {
        this.graph = getGraph();
    }

    private static Graph getGraph() {
        ParquetStoreProperties pp = (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                AbstractOperationsTest.class.getResourceAsStream("/multiUseStore.properties"));
        return new Graph.Builder()
                .addSchema(getSchema())
                .storeProperties(pp)
                .build();
    }
    
    protected static Schema getSchema() {
        return Schema.fromJson(LongVertexOperationsTest.class.getResourceAsStream("/schemaUsingLongVertexType/dataSchema.json"),
                LongVertexOperationsTest.class.getResourceAsStream("/schemaUsingLongVertexType/dataTypes.json"),
                LongVertexOperationsTest.class.getResourceAsStream("/schemaUsingLongVertexType/storeSchema.json"),
                LongVertexOperationsTest.class.getResourceAsStream("/schemaUsingLongVertexType/storeTypes.json"));
    }

    private static Iterable<Element> getElements() {
        return DataGen.generate300LongElements();
    }

    @Override
    public void setupSeeds() {
        this.seedsList = new ArrayList<>(5);
        seedsList.add(new EntitySeed(5L));
        seedsList.add(new EntitySeed(15L));
        seedsList.add(new EntitySeed(10L));
        seedsList.add(new EdgeSeed(13L, 14L, true));
        seedsList.add(new EdgeSeed(2L, 3L, true));
    }

    @Override
    public void setupView() {
        this.view = new View.Builder()
                .edge("BasicEdge",
                    new ViewElementDefinition.Builder()
                        .preAggregationFilter(
                            new ElementFilter.Builder()
                                .select("property4_cardinality", "property2")
                                .execute(
                                    new Or.Builder()
                                        .select(0)
                                        .execute(new IsLessThan(2L, true))
                                        .select(1)
                                        .execute(new IsMoreThan(3.0, true))
                                        .build())
                                .build())
                        .transientProperty("property4_cardinality", Long.class)
                        .build())
                .entity("BasicEntity",
                    new ViewElementDefinition.Builder().preAggregationFilter(
                        new ElementFilter.Builder()
                            .select("property8.cardinality")
                            .execute(
                                new Not(new IsMoreThan(2L, false)))
                            .build())
                    .transientProperty("property8.cardinality", Long.class).build())
                .build();
    }

    @Override
    protected void checkData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        Edge edge = (Edge) dataIter.next();
        Date date = (Date) edge.getProperty("property7");
        checkEdge(edge, "BasicEdge", 0L, 1L, false, (byte) 'a', 0.0, 2f, 2L, 5L, (short) 6, date, 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 0L, 1L, false, (byte) 'b', 0.3, 4f, 2L, 0L, (short) 7, new Date(date.getTime() + 1000), 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 0L, 1L, true, (byte) 'b', 0.3, 6f, 3L, 5L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 1L, 2L, false, (byte) 'a', 0.2, 2f, 2L, 5L, (short) 6, date, 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 1L, 2L, false, (byte) 'b', 0.3, 4f, 2L, 6L, (short) 7, new Date(date.getTime() + 1000), 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 1L, 2L, true, (byte) 'b', 0.5, 6f, 3L, 11L, (short) 13, date, 2);
        for (int i = 0; i < 69; i++) {
            dataIter.next();
        }
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 0L, 1L, false, (byte) 'b', 0.3, 6f, 3L, 5L, (short) 13, new Date(date.getTime() + 1000), 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 0L, 1L, true, (byte) 'b', 0.3, 6f, 3L, 5L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 1L, 2L, false, (byte) 'b', 0.5, 6f, 3L, 11L, (short) 13, new Date(date.getTime() + 1000), 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 1L, 2L, true, (byte) 'b', 0.5, 6f, 3L, 11L, (short) 13, date, 2);
        for (int i = 0; i < 46; i++) {
            dataIter.next();
        }
        Entity entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", 0L, (byte) 'b', 0.5, 7f, 3L, 0L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", 1L, (byte) 'b', 0.5, 7f, 3L, 11L, (short) 13, date, 2);
        for (int i = 0; i < 23; i++) {
            dataIter.next();
        }
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", 0L, (byte) 'b', 0.5, 7f, 3L, 0L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", 1L, (byte) 'b', 0.5, 7f, 3L, 11L, (short) 13, date, 2);
        for (int i = 0; i < 23; i++) {
            dataIter.next();
        }
        assertFalse(dataIter.hasNext());
    }
    
    @Override
    void checkGetSeededElementsData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        Edge edge = (Edge) dataIter.next();
        Date date = (Date) edge.getProperty("property7");
        checkEdge(edge, "BasicEdge", 2L, 3L, true, (byte) 'b', 0.7, 6f, 3L, 17L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 5L, 6L, false, (byte) 'a', 1.0, 2f, 2L, 5L, (short) 6, date, 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 5L, 6L, false, (byte) 'b', 0.3, 4f, 2L, 30L, (short) 7, new Date(date.getTime() + 1000), 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 5L, 6L, true, (byte) 'b', 1.3, 6f, 3L, 35L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 10L, 11L, false, (byte) 'a', 2.0, 2f, 2L, 5L, (short) 6, date, 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 10L, 11L, false, (byte) 'b', 0.3, 4f, 2L, 60L, (short) 7, new Date(date.getTime() + 1000), 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 10L, 11L, true, (byte) 'b', 2.3, 6f, 3L, 65L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 13L, 14L, true, (byte) 'b', 2.9, 6f, 3L, 83L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 15L, 16L, false, (byte) 'a', 3.0, 2f, 2L, 5L, (short) 6, date, 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 15L, 16L, false, (byte) 'b', 0.3, 4f, 2L, 90L, (short) 7, new Date(date.getTime() + 1000), 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 15L, 16L, true, (byte) 'b', 3.3, 6f, 3L, 95L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 2L, 3L, true, (byte) 'b', 0.7, 6f, 3L, 17L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 5L, 6L, false, (byte) 'b', 1.3, 6f, 3L, 35L, (short) 13, new Date(date.getTime() + 1000), 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 5L, 6L, true, (byte) 'b', 1.3, 6f, 3L, 35L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 10L, 11L, false, (byte) 'b', 2.3, 6f, 3L, 65L, (short) 13, new Date(date.getTime() + 1000), 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 10L, 11L, true, (byte) 'b', 2.3, 6f, 3L, 65L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 13L, 14L, true, (byte) 'b', 2.9, 6f, 3L, 83L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 15L, 16L, false, (byte) 'b', 3.3, 6f, 3L, 95L, (short) 13, new Date(date.getTime() + 1000), 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 15L, 16L, true, (byte) 'b', 3.3, 6f, 3L, 95L, (short) 13, date, 2);
        Entity entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", 2L, (byte) 'b', 0.5, 7f, 3L, 22L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", 3L, (byte) 'b', 0.5, 7f, 3L, 33L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", 5L, (byte) 'b', 0.5, 7f, 3L, 55L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", 10L, (byte) 'b', 0.5, 7f, 3L, 110L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", 13L, (byte) 'b', 0.5, 7f, 3L, 143L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", 14L, (byte) 'b', 0.5, 7f, 3L, 154L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", 15L, (byte) 'b', 0.5, 7f, 3L, 165L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", 2L, (byte) 'b', 0.5, 7f, 3L, 22L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", 3L, (byte) 'b', 0.5, 7f, 3L, 33L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", 5L, (byte) 'b', 0.5, 7f, 3L, 55L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", 10L, (byte) 'b', 0.5, 7f, 3L, 110L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", 13L, (byte) 'b', 0.5, 7f, 3L, 143L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", 14L, (byte) 'b', 0.5, 7f, 3L, 154L, (short) 13, date, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", 15L, (byte) 'b', 0.5, 7f, 3L, 165L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 4L, 5L, false, (byte) 'a', 0.8, 2f, 2L, 5L, (short) 6, date, 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 4L, 5L, false, (byte) 'b', 0.3, 4f, 2L, 24L, (short) 7, new Date(date.getTime() + 1000), 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 4L, 5L, true, (byte) 'b', 1.1, 6f, 3L, 29L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 9L, 10L, false, (byte) 'a', 1.8, 2f, 2L, 5L, (short) 6, date, 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 9L, 10L, false, (byte) 'b', 0.3, 4f, 2L, 54L, (short) 7, new Date(date.getTime() + 1000), 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 9L, 10L, true, (byte) 'b', 2.1, 6f, 3L, 59L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 14L, 15L, false, (byte) 'a', 2.8, 2f, 2L, 5L, (short) 6, date, 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 14L, 15L, false, (byte) 'b', 0.3, 4f, 2L, 84L, (short) 7, new Date(date.getTime() + 1000), 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 14L, 15L, true, (byte) 'b', 3.1, 6f, 3L, 89L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 4L, 5L, false, (byte) 'b', 1.1, 6f, 3L, 29L, (short) 13, new Date(date.getTime() + 1000), 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 4L, 5L, true, (byte) 'b', 1.1, 6f, 3L, 29L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 9L, 10L, false, (byte) 'b', 2.1, 6f, 3L, 59L, (short) 13, new Date(date.getTime() + 1000), 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 9L, 10L, true, (byte) 'b', 2.1, 6f, 3L, 59L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 14L, 15L, false, (byte) 'b', 3.1, 6f, 3L, 89L, (short) 13, new Date(date.getTime() + 1000), 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", 14L, 15L, true, (byte) 'b', 3.1, 6f, 3L, 89L, (short) 13, date, 2);
        assertFalse(dataIter.hasNext());
    }

    @Override
    void checkGetFilteredElementsData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        Edge edge = (Edge) dataIter.next();
        Date date = (Date) edge.getProperty("property7");
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 15L, 16L, false, (byte) 'a', 3.0, 2f, 2L, 5L, (short) 6, date, 1);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 15L, 16L, true, (byte) 'b', 3.3, 6f, 3L, 95L, (short) 13, date, 2);
        for (int i = 0; i < 17; i++) {
            dataIter.next();
        }
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 24L, 25L, true, (byte) 'b', 5.1, 6f, 3L, 149L, (short) 13, date, 2);
        assertFalse(dataIter.hasNext());
    }

    @Override
    void checkGetSeededAndFilteredElementsData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        Edge edge = (Edge) dataIter.next();
        Date date = (Date) edge.getProperty("property7");
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 15L, 16L, true, (byte) 'b', 3.3, 6f, 3L, 95L, (short) 13, date, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", 14L, 15L, true, (byte) 'b', 3.1, 6f, 3L, 89L, (short) 13, date, 2);
        assertFalse(dataIter.hasNext());
    }
}
