/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.translator.Translator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;

import java.util.Map;
import java.util.Optional;

public class GremlinQueryUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinQueryUtils.class);

    private GremlinQueryUtils() {
        // Utility class
    }

    public static void parseOptionsAndUpdateTraversal(Admin<?, ?> traversal) {
        // Check for any options on the traversal
        Optional<OptionsStrategy> optionsOptional = traversal.getStrategies().getStrategy(OptionsStrategy.class);
        if (!optionsOptional.isPresent()) {
            return;
        }
        Map<String, Object> options = optionsOptional.get().getOptions();
        GafferPopGraph graph = (GafferPopGraph) traversal.getGraph().get();

        // Restore variables to defaults before parsing options
        graph.setDefaultVariables((GafferPopGraphVariables) graph.variables());

        // Add the options to the graph variables for use elsewhere
        options.forEach((k, v) -> {
            if (graph.variables().asMap().containsKey(k)) {
                graph.variables().set(k, v);
            }
        });

        // Translate and add a cypher traversal in if that key has been set
        if (options.containsKey(GafferPopGraphVariables.CYPHER_KEY)) {
            LOGGER.info("Replacing traversal with translated Cypher query");
            CypherAst ast = CypherAst.parse((String) options.get(GafferPopGraphVariables.CYPHER_KEY));
            Admin<?, ?> translatedCypher = ast.buildTranslation(Translator.builder().traversal().enableCypherExtensions().build()).asAdmin();

            // Add the cypher traversal
            TraversalHelper.insertTraversal(0, translatedCypher, traversal);
            LOGGER.debug("New traversal is: {}", traversal);
        }
    }

}
