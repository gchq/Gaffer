/*
 * Copyright 2017-2019 Crown Copyright
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
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.traffic.generator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.traffic.ElementGroup;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RoadTrafficDataLoaderITs {

    // Example lines taken from 2015 issue of GB Road Traffic Counts data set (raw count - major roads)
    private static final String CSV_HEADER_V_1 = "\"Region Name (GO)\",\"ONS LACode\",\"ONS LA Name\",\"CP\",\"S Ref E\",\"S Ref N\",\"Road\",\"A-Junction\",\"A Ref E\",\"A Ref N\",\"B-Junction\",\"B Ref E\",\"B Ref N\",\"RCat\",\"iDir\",\"Year\",\"dCount\",\"Hour\",\"PC\",\"2WMV\",\"CAR\",\"BUS\",\"LGV\",\"HGVR2\",\"HGVR3\",\"HGVR4\",\"HGVA3\",\"HGVA5\",\"HGVA6\",\"HGV\",\"AMV\"";
    private static final String CSV_LINE_V_1 = "\"South West\",\"E06000054\",\"Wiltshire\",\"6016\",\"389200\",\"179080\",\"M4\",\"LA Boundary\",\"381800\",\"180030\",\"17\",\"391646\",\"179560\",\"TM\",\"E\",\"2000\",\"2000-05-03 00:00:00\",\"7\",\"0\",\"9\",\"2243\",\"15\",\"426\",\"127\",\"21\",\"20\",\"37\",\"106\",\"56\",\"367\",\"3060\"";

    // Example lines taken from 2016 issue of GB Road Traffic Counts data set (raw count - major roads)
    private static final String CSV_HEADER_V_2 = "Region Name (GO),ONS LACode,ONS LA Name,CP,S Ref E,S Ref N,S Ref Latitude,S Ref Longitude,Road,A-Junction,A Ref E,A Ref N,B-Junction,B Ref E,B Ref N,RCat,iDir,Year,dCount,Hour,PC,2WMV,CAR,BUS,LGV,HGVR2,HGVR3,HGVR4,HGVA3,HGVA5,HGVA6,HGV,AMV";
    private static final String CSV_LINE_V_2 = "\"Wales\",W06000022,\"Newport\",501,328570,187000,51.577320306,-3.032184269,M4,\"28\",328380,185830,\"27\",328400,187800,TM,E,2000,2000-06-09 00:00:00,7,0,6,2491,33,539,164,25,22,30,91,59,391,3460";

    @Rule
    public final TestName testName = new TestName();

    @Test
    public void shouldLoadCsvV1Line() throws IOException, OperationException {
        final InputStream storeProps = StreamUtil.openStream(getClass(), "/mockaccumulo.properties");
        final InputStream[] schema = StreamUtil.schemas(ElementGroup.class);
        final User user = new User();

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(this.testName.getMethodName())
                        .build())
                .storeProperties(storeProps)
                .addSchemas(schema)
                .build();

        // Load data
        final RoadTrafficDataLoader dataLoader = new RoadTrafficDataLoader(graph, user);
        dataLoader.load(CSV_HEADER_V_1 + "\n" + CSV_LINE_V_1);

        // Check data has been loaded
        final CloseableIterable<? extends Element> elements = graph.execute(new GetAllElements.Builder().build(), user);

        int entityCount = 0;
        int edgeCount = 0;
        for (final Element element : elements) {
            if (element instanceof Entity) {
                entityCount++;
            } else if (element instanceof Edge) {
                edgeCount++;
            } else {
                fail("Unrecognised element class: " + element.getClassName());
            }
        }

        assertEquals(15, entityCount);
        assertEquals(7, edgeCount);
    }

    @Test
    public void shouldLoadCsvV2Line() throws IOException, OperationException {
        final InputStream storeProps = StreamUtil.openStream(getClass(), "/mockaccumulo.properties");
        final InputStream[] schema = StreamUtil.schemas(ElementGroup.class);
        final User user = new User();

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(this.testName.getMethodName())
                        .build())
                .storeProperties(storeProps)
                .addSchemas(schema)
                .build();

        // Load data
        final RoadTrafficDataLoader dataLoader = new RoadTrafficDataLoader(graph, user);
        dataLoader.load(CSV_HEADER_V_2 + "\n" + CSV_LINE_V_2);

        // Check data has been loaded
        final CloseableIterable<? extends Element> elements = graph.execute(new GetAllElements.Builder().build(), user);

        int entityCount = 0;
        int edgeCount = 0;
        for (final Element element : elements) {
            if (element instanceof Entity) {
                entityCount++;
            } else if (element instanceof Edge) {
                edgeCount++;
            } else {
                fail("Unrecognised element class: " + element.getClassName());
            }
        }

        assertEquals(15, entityCount);
        assertEquals(7, edgeCount);
    }

}
