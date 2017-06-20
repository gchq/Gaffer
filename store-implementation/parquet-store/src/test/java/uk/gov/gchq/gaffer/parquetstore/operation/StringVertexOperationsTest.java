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
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
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

public class StringVertexOperationsTest extends AbstractOperationsTest {

    @BeforeClass
    public static void genData() throws OperationException {
        Logger.getRootLogger().setLevel(Level.WARN);
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
        return Schema.fromJson(StringVertexOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                StringVertexOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                StringVertexOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                StringVertexOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
    }


    private static Iterable<? extends Element> getElements() {
        return DataGen.generate300StringElementsWithNullProperties();
    }

    @Override
    public void setupSeeds() {
        this.seedsList = new ArrayList<>(6);
        seedsList.add(new EntitySeed("src5"));
        seedsList.add(new EntitySeed("dst15"));
        seedsList.add(new EntitySeed("vert10"));
        seedsList.add(new EdgeSeed("src13", "dst13", true));
        seedsList.add(new EntitySeed("dst7"));
        seedsList.add(new EntitySeed("src2"));
    }

    @Override
    public void setupView() {
        this.view = new View.Builder()
            .edge("BasicEdge",
                new ViewElementDefinition.Builder()
                    .preAggregationFilter(
                        new ElementFilter.Builder()
                            .select(ParquetStoreConstants.SOURCE)
                            .execute(new Or(new IsLessThan("src12", true), new IsMoreThan("src4", true)))
                            .build())
                    .build())
            .entity("BasicEntity",
                new ViewElementDefinition.Builder()
                    .preAggregationFilter(
                        new ElementFilter.Builder()
                            .select(ParquetStoreConstants.VERTEX)
                            .execute(
                                new Not(new IsMoreThan("vert12", false)))
                            .build())
                .build())
            .build();
    }

    @Override
    protected void checkData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        int counter = 0;
        while (dataIter.hasNext()) {
            dataIter.next();
            counter++;
        }
        assertEquals(150, counter);
    }

    @Override
    void checkGetSeededElementsData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        int counter = 0;
        while (dataIter.hasNext()) {
            dataIter.next();
            counter++;
        }
        assertEquals(18, counter);
    }

    @Override
    void checkGetFilteredElementsData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        int counter = 0;
        while (dataIter.hasNext()) {
            dataIter.next();
            counter++;
        }
        assertEquals(27, counter);
    }

    @Override
    void checkGetSeededAndFilteredElementsData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        int counter = 0;
        while (dataIter.hasNext()) {
            dataIter.next();
            counter++;
        }
        assertEquals(5, counter);
    }
}
