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
import uk.gov.gchq.gaffer.data.element.Element;
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
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LongVertexOperationsTest extends AbstractOperationsTest {
    
    @BeforeClass
    public static void genData() throws OperationException {
        Logger.getRootLogger().setLevel(Level.WARN);
        getGraph().execute(new AddElements.Builder().input(getElements()).build(), USER);
    }

    @Before
    public void setup() {
        graph = getGraph();
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
        return Schema.fromJson(
                LongVertexOperationsTest.class.getResourceAsStream("/schemaUsingLongVertexType/dataSchema.json"),
                LongVertexOperationsTest.class.getResourceAsStream("/schemaUsingLongVertexType/dataTypes.json"),
                LongVertexOperationsTest.class.getResourceAsStream("/schemaUsingLongVertexType/storeSchema.json"),
                LongVertexOperationsTest.class.getResourceAsStream("/schemaUsingLongVertexType/storeTypes.json"));
    }

    private static Iterable<Element> getElements() {
        return DataGen.generate300LongElements();
    }

    @Override
    public void setupSeeds() {
        seedsList = new ArrayList<>(5);
        seedsList.add(new EntitySeed(5L));
        seedsList.add(new EntitySeed(15L));
        seedsList.add(new EntitySeed(10L));
        seedsList.add(new EdgeSeed(13L, 14L, true));
        seedsList.add(new EdgeSeed(2L, 3L, true));
    }

    @Override
    public void setupView() {
        view = new View.Builder()
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
    protected void checkData(final CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        int counter = 0;
        while (dataIter.hasNext()) {
            dataIter.next();
            counter++;
        }
        assertEquals(175, counter);
    }
    
    @Override
    void checkGetSeededElementsData(final CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        int counter = 0;
        while (dataIter.hasNext()) {
            dataIter.next();
            counter++;
        }
        assertEquals(48, counter);
    }

    @Override
    void checkGetFilteredElementsData(final CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        int counter = 0;
        while (dataIter.hasNext()) {
            dataIter.next();
            counter++;
        }
        assertEquals(21, counter);
    }

    @Override
    void checkGetSeededAndFilteredElementsData(final CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        int counter = 0;
        while (dataIter.hasNext()) {
            dataIter.next();
            counter++;
        }
        assertEquals(3, counter);
    }
}
