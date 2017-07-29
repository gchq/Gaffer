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
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class StringVertexOperationsTest extends AbstractOperationsTest {

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
                .graphId("test")
                .build();
    }

    protected static Schema getSchema() {
        return Schema.fromJson(
                StringVertexOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                StringVertexOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                StringVertexOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                StringVertexOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
    }
    
    private static Iterable<? extends Element> getElements() {
        return DataGen.generate300StringElementsWithNullProperties();
    }

    @Override
    public void setupSeeds() {
        seedsList = new ArrayList<>(6);
        seedsList.add(new EntitySeed("src5"));
        seedsList.add(new EntitySeed("dst15"));
        seedsList.add(new EntitySeed("vert10"));
        seedsList.add(new EdgeSeed("src13", "dst13", true));
        seedsList.add(new EntitySeed("dst7"));
        seedsList.add(new EntitySeed("src2"));
    }

    @Override
    public void setupView() {
        view = new View.Builder()
            .edge("BasicEdge",
                new ViewElementDefinition.Builder()
                    .preAggregationFilter(
                        new ElementFilter.Builder()
                            .select(ParquetStoreConstants.SOURCE)
                            .execute(new Or<>(new IsLessThan("src12", true), new IsMoreThan("src4", true)))
                            .build())
                    .build())
            .entity("BasicEntity",
                new ViewElementDefinition.Builder()
                    .preAggregationFilter(
                        new ElementFilter.Builder()
                            .select(ParquetStoreConstants.VERTEX)
                            .execute(
                                new Not<>(new IsMoreThan("vert12", false)))
                            .build())
                .build())
            .build();
    }

    @Override
    protected void checkData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(150);
        final List<Element> actual = new ArrayList<>(150);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        for (int i = 0; i < 25; i++) {
            expected.add(DataGen.getEdge("BasicEdge", "src" + i, "dst" + i, true, null, null, null, null, null, null, null, null, 2));
            expected.add(DataGen.getEdge("BasicEdge", "src" + i, "dst" + i, false, null, null, null, null, null, null, null, null, 2));

            expected.add(DataGen.getEdge("BasicEdge2", "src" + i, "dst" + i, true, null, null, null, null, null, null, null, null, 2));
            expected.add(DataGen.getEdge("BasicEdge2", "src" + i, "dst" + i, false, null, null, null, null, null, null, null, null, 2));

            expected.add(DataGen.getEntity("BasicEntity", "vert" + i, null, null, null, null, null, null, null, null, 2));
            expected.add(DataGen.getEntity("BasicEntity2", "vert" + i, null, null, null, null, null, null, null, null, 2));
        }
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Override
    void checkGetSeededElementsData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(20);
        final List<Element> actual = new ArrayList<>(20);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        expected.add(DataGen.getEdge("BasicEdge", "src13", "dst13", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src2", "dst2", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src2", "dst2", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src5", "dst5", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src5", "dst5", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge2", "src13", "dst13", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge2", "src2", "dst2", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge2", "src2", "dst2", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge2", "src5", "dst5", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge2", "src5", "dst5", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEntity("BasicEntity", "vert10", null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEntity("BasicEntity2", "vert10", null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src15", "dst15", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src15", "dst15", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src7", "dst7", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src7", "dst7", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge2", "src15", "dst15", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge2", "src15", "dst15", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge2", "src7", "dst7", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge2", "src7", "dst7", false, null, null, null, null, null, null, null, null, 2));

        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Override
    void checkGetFilteredElementsData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(41);
        final List<Element> actual = new ArrayList<>(41);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        expected.add(DataGen.getEdge("BasicEdge", "src0", "dst0", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src0", "dst0", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src1", "dst1", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src1", "dst1", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src10", "dst10", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src10", "dst10", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src11", "dst11", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src11", "dst11", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src12", "dst12", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src12", "dst12", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src13", "dst13", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src14", "dst14", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src15", "dst15", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src16", "dst16", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src17", "dst17", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src18", "dst18", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src19", "dst19", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src2", "dst2", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src20", "dst20", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src21", "dst21", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src22", "dst22", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src23", "dst23", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src24", "dst24", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src3", "dst3", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src4", "dst4", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src4", "dst4", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src5", "dst5", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src5", "dst5", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src6", "dst6", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src6", "dst6", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src7", "dst7", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src7", "dst7", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src8", "dst8", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src8", "dst8", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src9", "dst9", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src9", "dst9", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEntity("BasicEntity", "vert0", null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEntity("BasicEntity", "vert1", null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEntity("BasicEntity", "vert10", null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEntity("BasicEntity", "vert11", null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEntity("BasicEntity", "vert12", null, null, null, null, null, null, null, null, 2));

        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Override
    void checkGetSeededAndFilteredElementsData(final CloseableIterable<? extends Element> data) {
        final List<Element> expected = new ArrayList<>(7);
        final List<Element> actual = new ArrayList<>(7);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        expected.add(DataGen.getEdge("BasicEdge", "src2", "dst2", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src15", "dst15", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src5", "dst5", false, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src5", "dst5", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEntity("BasicEntity", "vert10", null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src7", "dst7", true, null, null, null, null, null, null, null, null, 2));
        expected.add(DataGen.getEdge("BasicEdge", "src7", "dst7", false, null, null, null, null, null, null, null, null, 2));

        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }
}
