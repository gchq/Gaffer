/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package gaffer.gafferpop;

import gaffer.commonutil.StreamUtil;
import gaffer.graph.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import java.util.Arrays;

public class GafferPopGraphTest {
    private static final Configuration TEST_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(GafferPopGraph.GRAPH, GafferPopGraph.class.getName());
    }};

    @Test
    public void shouldAddAndGetElements2() {
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);

        // add a software vertex with a name property
        Vertex gremlin = graph.addVertex(T.label, "software", T.id, 1, "name", "gremlin");
        // only one vertex should exist
        assert (IteratorUtils.count(graph.vertices(Arrays.asList(1, 2, 3, 4, 5, 6), "software")) == 1);
        // no edges should exist as none have been created
        assert (IteratorUtils.count(graph.edges(new EdgeId(1, 2))) == 0);
        // add a new software vertex to the graph
        Vertex blueprints = graph.addVertex(T.label, "software", T.id, 2, "name", "blueprints");
        // connect gremlin to blueprints via a dependsOn-edge
        gremlin.addEdge("dependsOn", blueprints);
        // now there are two verticesWithView and one edge
        assert (IteratorUtils.count(graph.vertices(Arrays.asList(1, 2, 3, 4, 5), "software")) == 2);
        assert (IteratorUtils.count(graph.edges(new EdgeId(1, 2))) == 1);
        // connect gremlin to blueprints via encapsulates
        gremlin.addEdge("encapsulates", blueprints);
        assert (IteratorUtils.count(graph.vertices(Arrays.asList(1, 2, 3, 4, 5), "software")) == 2);
        assert (IteratorUtils.count(graph.edges(new EdgeId(1, 2))) == 2);
    }

    private Graph getGafferGraph() {
        return new Graph.Builder()
                .storeProperties(StreamUtil.openStream(this.getClass(), "/gaffer/store.properties", true))
                .addSchema(StreamUtil.openStream(this.getClass(), "/gaffer/schema/dataSchema.json", true))
                .addSchema(StreamUtil.openStream(this.getClass(), "/gaffer/schema/dataTypes.json", true))
                .addSchema(StreamUtil.openStream(this.getClass(), "/gaffer/schema/storeTypes.json", true))
                .build();
    }

}