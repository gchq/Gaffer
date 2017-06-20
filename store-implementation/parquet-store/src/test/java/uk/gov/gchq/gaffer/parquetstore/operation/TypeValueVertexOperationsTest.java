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
import uk.gov.gchq.gaffer.types.TypeValue;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Or;

import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeValueVertexOperationsTest extends AbstractOperationsTest {
    
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
        return Schema.fromJson(TypeValueVertexOperationsTest.class.getResourceAsStream("/schemaUsingTypeValueVertexType/dataSchema.json"),
                TypeValueVertexOperationsTest.class.getResourceAsStream("/schemaUsingTypeValueVertexType/dataTypes.json"),
                TypeValueVertexOperationsTest.class.getResourceAsStream("/schemaUsingTypeValueVertexType/storeSchema.json"),
                TypeValueVertexOperationsTest.class.getResourceAsStream("/schemaUsingTypeValueVertexType/storeTypes.json"));
    }

    private static Iterable<? extends Element> getElements() {
        return DataGen.generate300TypeValueElements();
    }

    @Override
    public void setupSeeds() {
        this.seedsList = new ArrayList<>(4);
        seedsList.add(new EntitySeed(new TypeValue("type0", "vrt10")));
        seedsList.add(new EntitySeed(new TypeValue("type2", "src17")));
        seedsList.add(new EdgeSeed(new TypeValue("type1", "src11"), new TypeValue("type1", "dst12"), true));
        seedsList.add(new EdgeSeed(new TypeValue("type1", "src1"), new TypeValue("type1", "dst2"), false));
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
                new ViewElementDefinition.Builder()
                    .preAggregationFilter(
                        new ElementFilter.Builder()
                            .select(ParquetStoreConstants.VERTEX + "_type")
                            .execute(new IsEqual("type0"))
                            .build())
                    .transientProperty(ParquetStoreConstants.VERTEX + "_type", String.class)
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
        assertEquals(175, counter);
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
        assertEquals(12, counter);
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
        assertEquals(21, counter);
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
        assertEquals(2, counter);
    }
}
