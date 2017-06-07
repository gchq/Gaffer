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
import uk.gov.gchq.gaffer.parquetstore.utils.Constants;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;

import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class StringVertexOperationsTest extends AbstractOperationsTest {

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
                            .select(Constants.SOURCE)
                            .execute(new Or(new IsLessThan("src12", true), new IsMoreThan("src4", true)))
                            .build())
                    .build())
            .entity("BasicEntity",
                new ViewElementDefinition.Builder()
                    .preAggregationFilter(
                        new ElementFilter.Builder()
                            .select(Constants.VERTEX)
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
        Edge edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src0", "dst0", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src0", "dst0", true, null, null, null, null, null, null, null, 2);
        for (int i = 0; i < 46; i++) {
            dataIter.next();
        }
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src9", "dst9", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src9", "dst9", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src0", "dst0", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src0", "dst0", true, null, null, null, null, null, null, null, 2);
        for (int i = 0; i < 46; i++) {
            dataIter.next();
        }
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src9", "dst9", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src9", "dst9", true, null, null, null, null, null, null, null, 2);
        Entity entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", "vert0", null, null, null, null, null, null, null, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", "vert1", null, null, null, null, null, null, null, 2);
        for (int i = 0; i < 23; i++) {
            dataIter.next();
        }
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", "vert0", null, null, null, null, null, null, null, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", "vert1", null, null, null, null, null, null, null, 2);
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
        checkEdge(edge, "BasicEdge", "src13", "dst13", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src2", "dst2", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src5", "dst5", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src5", "dst5", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src13", "dst13", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src2", "dst2", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src5", "dst5", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src5", "dst5", true, null, null, null, null, null, null, null, 2);
        Entity entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", "vert10", null, null, null, null, null, null, null, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity2", "vert10", null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src15", "dst15", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src15", "dst15", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src7", "dst7", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src7", "dst7", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src15", "dst15", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src15", "dst15", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src7", "dst7", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge2", "src7", "dst7", true, null, null, null, null, null, null, null, 2);
        assertFalse(dataIter.hasNext());
    }

    @Override
    void checkGetFilteredElementsData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        Edge edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src0", "dst0", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src0", "dst0", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src1", "dst1", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src1", "dst1", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src10", "dst10", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src10", "dst10", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src11", "dst11", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src11", "dst11", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src12", "dst12", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src12", "dst12", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src4", "dst4", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src4", "dst4", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src5", "dst5", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src5", "dst5", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src6", "dst6", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src6", "dst6", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src7", "dst7", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src7", "dst7", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src8", "dst8", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src8", "dst8", true, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src9", "dst9", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src9", "dst9", true, null, null, null, null, null, null, null, 2);
        Entity entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", "vert0", null, null, null, null, null, null, null, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", "vert1", null, null, null, null, null, null, null, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", "vert10", null, null, null, null, null, null, null, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", "vert11", null, null, null, null, null, null, null, 2);
        entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", "vert12", null, null, null, null, null, null, null, 2);
        assertFalse(dataIter.hasNext());
    }

    @Override
    void checkGetSeededAndFilteredElementsData(CloseableIterable<? extends Element> data) {
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        Edge edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src5", "dst5", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src5", "dst5", true, null, null, null, null, null, null, null, 2);
        Entity entity = (Entity) dataIter.next();
        checkEntity(entity, "BasicEntity", "vert10", null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src7", "dst7", false, null, null, null, null, null, null, null, 2);
        edge = (Edge) dataIter.next();
        checkEdge(edge, "BasicEdge", "src7", "dst7", true, null, null, null, null, null, null, null, 2);
        assertFalse(dataIter.hasNext());
    }
}
