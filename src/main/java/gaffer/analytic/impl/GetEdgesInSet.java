/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.analytic.impl;

import gaffer.CloseableIterable;
import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.utils.Accumulo;
import gaffer.accumulo.utils.AccumuloConfig;
import gaffer.analytic.Analytic;
import gaffer.analytic.parameters.Parameters;
import gaffer.analytic.parameters.TypeValueSet;
import gaffer.analytic.result.GraphElementWithStatisticsIterableResult;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;
import gaffer.graph.output.GraphElementWithStatisticsFormatter;
import gaffer.graph.output.StandardGraphElementWithStatisticsFormatter;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Retrieves all {@link Edge}s that are between the provided set of {@link TypeValue}s. Also optionally returns
 * all {@link Entity}s for the provided set.
 */
public class GetEdgesInSet implements Analytic {

    private final static String NAME = "Gets all edges between the seeds";
    private final static String DESCRIPTION = "Returns all edges for which both ends are in the provided set of seeds. "
            + "Also returns any entities for the provided seeds if desired.";

    private TypeValueSet seeds;
    private CloseableIterable<GraphElementWithStatistics> retriever;

    public GetEdgesInSet() {
        this.retriever = null;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    @Override
    public void setParameters(Parameters parameters) throws IllegalArgumentException {
        if (!(parameters instanceof TypeValueSet)) {
            throw new IllegalArgumentException("Need to supply a " + TypeValueSet.class.getName() + " as parameters.");
        }
        this.seeds = (TypeValueSet) parameters;
        // Check that all parameters have been set
        if (this.seeds == null) {
            throw new IllegalArgumentException("Need to supply non-null seeds.");
        }
        if (this.seeds.getAccumuloGraph() == null) {
            throw new IllegalArgumentException("Need to supply non-null AccumuloBackedGraph.");
        }
    }

    @Override
    public GraphElementWithStatisticsIterableResult getResult() throws IOException {
        retriever = this.seeds.getAccumuloGraph().getGraphElementsWithStatisticsWithinSet(this.seeds.getTypeValues(), true);
        return new GraphElementWithStatisticsIterableResult(retriever);
    }

    @Override
    public void close() {
        if (retriever != null) {
            retriever.close();
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            // Usage
            if (args.length != 4 && args.length != 5) {
                System.err.println("Usage: " + GetEdgesInSet.class.getName()
                        + " <accumulo_properties_file> <seeds_file> <roll_up_over_time_and_visibility - true|false>"
                        + " <edge|entity|both>"
                        + " <optional - fully qualified name of formatter>");
                System.err.println("Here:\n\tseeds_file should contain one TypeValue per line, in the form type|value.");
                System.err.println("\troll_up_over_time_and_visibility indicates whether edges should be merged over time and visibility.");
                System.err.println("\tedge|entity|both indicates whether the user wants to see only entities, or only edges or both.");
                System.err.println("\toptionally the class name of a formatter can be supplied - this allows command-line users of Gaffer to specify their own output format.");
                return;
            }

            // Get properties from args
            String accumuloPropertiesFile = args[0];
            String seedsFile = args[1];
            if (!args[2].equalsIgnoreCase("true") && !args[2].equalsIgnoreCase("false")) {
                System.err.println("Third argument should be true or false.");
                return;
            }
            boolean rollUpOverTimeAndVisibility = Boolean.parseBoolean(args[2]);
            String entityOrEdgeOrBoth = "both";
            if (args[3].equals("entity") || args[3].equals("edge") || args[3].equals("both")) {
                entityOrEdgeOrBoth = args[3];
            } else {
                System.err.println("Fourth argument should be 'entity', 'edge' or 'both'");
                return;
            }
            GraphElementWithStatisticsFormatter formatter;
            try {
                Class formatterClass = Class.forName(StandardGraphElementWithStatisticsFormatter.class.getName());
                if (args.length == 5) {
                    formatterClass = Class.forName(args[4]);
                }
                Object obj;
                obj = formatterClass.newInstance();
                formatter = (GraphElementWithStatisticsFormatter) obj;
                if (!(obj instanceof GraphElementWithStatisticsFormatter)) {
                    System.err.println("Error: formatter class " + args[4] + " is not an instance of a GraphElementWithStatisticsFormatter.");
                    return;
                }
            } catch (ClassNotFoundException e) {
                System.err.println("Error: formatter class " + args[4] + " cannot be found: " + e.getMessage());
                return;
            } catch (InstantiationException e) {
                System.err.println("Error: formatter class " + args[4] + " cannot be instantiated: " + e.getMessage());
                return;
            } catch (IllegalAccessException e) {
                System.err.println("Error: formatter class " + args[4] + " causes IllegalAccessException: " + e.getMessage());
                return;
            }

            // Connect to Accumulo and sanity check that the table exists
            AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFile);
            Connector connector = Accumulo.connect(accConf);
            String tableName = accConf.getTable();
            String user = connector.whoami();
            Authorizations authorizations = connector.securityOperations().getUserAuthorizations(user);
            boolean tableExists = connector.tableOperations().exists(tableName);
            if (!tableExists) {
                System.err.println("Table " + tableName + " does not exist.");
                return;
            }
            System.out.println("Connected as user " + user + " to table " + tableName + " with the following authorizations:\n"
                    + authorizations);

            // Create AccumuloBackedGraph
            AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
            graph.rollUpOverTimeAndVisibility(rollUpOverTimeAndVisibility);
            graph.setMaxThreadsForBatchScanner(40);
            if (entityOrEdgeOrBoth.equals("entity")) {
                graph.setReturnEntitiesOnly();
            } else if (entityOrEdgeOrBoth.equals("edge")) {
                graph.setReturnEdgesOnly();
            } else {
                graph.setReturnEntitiesAndEdges();
            }

            // Read seeds
            System.out.println("Reading seeds from file " + seedsFile);
            Set<TypeValue> seeds = new HashSet<TypeValue>();
            BufferedReader reader = new BufferedReader(new FileReader(new File(seedsFile)));
            String line;
            int count = 0;
            int countMalformedSeeds = 0;
            while ( (line = reader.readLine()) != null ) {
                String[] splits = line.split("\\|");
                // Check that right number of fields
                if (splits.length != 2) {
                    countMalformedSeeds++;
                } else {
                    seeds.add(new TypeValue(splits[0], splits[1]));
                    count++;
                }
            }
            reader.close();
            System.out.println("Lines read = " + count);
            System.out.println("Malformed seeds = " + countMalformedSeeds);
            System.out.println("Number of distinct seeds read = " + seeds.size());

            // Create parameters from seeds
            TypeValueSet seedsSet = new TypeValueSet();
            seedsSet.setAccumuloGraph(graph);
            seedsSet.setTypeValues(seeds);

            // Create analytic
            GetEdgesInSet getEdgesInSet = new GetEdgesInSet();
            try {
                getEdgesInSet.setParameters(seedsSet);
            } catch (IllegalArgumentException e) {
                System.err.println("Invalid Parameters: " + e.getMessage());
                return;
            }

            // Get results
            for (GraphElementWithStatistics gews : getEdgesInSet.getResult().getResults()) {
                System.out.println(formatter.format(gews));
            }
            getEdgesInSet.close();

        } catch (IOException e) {
            System.err.println("IOException " + e.getMessage());
        } catch (AccumuloException e) {
            System.err.println("AccumuloException " + e.getMessage());
        } catch (AccumuloSecurityException e) {
            System.err.println("AccumuloSecurityException " + e.getMessage());
        }
    }

}
