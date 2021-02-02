/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.integration.config;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.integration.AbstractRestApiIT;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestPropertySource(
    properties = "gaffer.graph.factory.class=uk.gov.gchq.gaffer.rest.integration.config.JsonSerialisationConfigIT$SerialisationGraphFactory"
)
public class JsonSerialisationConfigIT extends AbstractRestApiIT {
    @Autowired
    private GraphFactory graphFactory;

    @Test
    public void shouldSerialiseHyperLogLogPlussesWhenSerialiserModulesConfigured() throws OperationException {
        // Given
        graphFactory.getGraph().execute(new AddElements.Builder()
            .input(new Entity.Builder()
                .vertex("vertex1")
                .group("Cardinality")
                .property("hllp", new HyperLogLogPlus(5, 5))
                .build())
            .build(), new User());

        // When
        ResponseEntity<List> elements = post("/graph/operations/execute", new GetAllElements(), List.class);
        Map<String, Object> result = ((List<Map<String, Object>>) elements.getBody()).get(0);
        Map<String, Object> hllp = ((Map<String, Map<String, Map<String, Map<String, Object>>>>) result.get("properties")).get("hllp").get(HyperLogLogPlus.class.getName()).get("hyperLogLogPlus");

        assertNotNull(hllp);
        assertTrue(hllp.containsKey("cardinality"));
    }


    public static class SerialisationGraphFactory implements GraphFactory {

        private Graph graph;

        @Override
        public Graph.Builder createGraphBuilder() {
            StoreProperties props = new MapStoreProperties();
            props.setJsonSerialiserModules(Sets.newHashSet(SketchesJsonModules.class));

            return new Graph.Builder()
                .addSchema(StreamUtil.openStream(getClass(), "/cardinalitySchema/schema.json"))
                .storeProperties(props)
                .config(new GraphConfig("graph"));
        }

        @Override
        public Graph getGraph() {
            if (graph == null) {
                graph = createGraph();
            }
            return graph;
        }
    }

}
