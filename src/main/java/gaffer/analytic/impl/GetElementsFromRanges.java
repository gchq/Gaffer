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
import gaffer.analytic.parameters.TypeValueRangeSet;
import gaffer.analytic.result.GraphElementWithStatisticsIterableResult;
import gaffer.graph.TypeValue;
import gaffer.graph.TypeValueRange;
import gaffer.graph.wrappers.GraphElementWithStatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves all {@link GraphElementWithStatistics} that involve any
 * {@link TypeValue}s from the provided set of {@link TypeValueRange}s.
 */
public class GetElementsFromRanges implements Analytic {

	private final static String NAME = "Get all graph elements involving type-values from the provided ranges";
	private final static String DESCRIPTION = "Returns all graph elements involving type-values from the provided ranges";

	private TypeValueRangeSet ranges;
	private CloseableIterable<GraphElementWithStatistics> retriever;

	public GetElementsFromRanges() {
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
		if (!(parameters instanceof TypeValueRangeSet)) {
			throw new IllegalArgumentException("Need to supply a " + TypeValueRangeSet.class.getName() + " as parameters.");
		}
		this.ranges = (TypeValueRangeSet) parameters;
		// Check that all parameters have been set
		if (this.ranges == null) {
			throw new IllegalArgumentException("Need to supply non-null ranges.");
		}
		if (this.ranges.getAccumuloGraph() == null) {
			throw new IllegalArgumentException("Need to supply non-null AccumuloBackeGraph.");
		}
	}

	@Override
	public GraphElementWithStatisticsIterableResult getResult() throws IOException {
		retriever = this.ranges.getAccumuloGraph().getGraphElementsWithStatisticsFromRanges(this.ranges.getTypeValueRanges());
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
		Logger logger = LoggerFactory.getLogger(GetElementsFromRanges.class);
		try {
			// Usage
			if (args.length != 3) {
				logger.error("Usage: " + GetElementsFromRanges.class.getName() + " <accumulo_properties_file> <ranges_file> <roll_up_over_time_and_visibility>\n"
						+ "Here ranges_file contains 1 range per line in the form starttype|startvalue|endtype|endvalue");
				return;
			}

			// Get properties from args
			String accumuloPropertiesFile = args[0];
			String seedsFile = args[1];
			if (!args[2].equalsIgnoreCase("true") && !args[2].equalsIgnoreCase("false")) {
				logger.error("Third argument should be true or false.");
				return;
			}
			boolean rollUpOverTimeAndVisibility = Boolean.parseBoolean(args[2]);

			// Connect to Accumulo and sanity check that the table exists
			AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFile);
			Connector connector = Accumulo.connect(accConf);
			String tableName = accConf.getTable();
			String user = connector.whoami();
			Authorizations authorizations = connector.securityOperations().getUserAuthorizations(user);
			boolean tableExists = connector.tableOperations().exists(tableName);
			if (!tableExists) {
				logger.error("Table " + tableName + " does not exist.");
				return;
			}
			logger.info("Connected as user " + user + " to table " + tableName + " with the following authorizations:\n"
					+ authorizations);

			// Create AccumuloBackedGraph
			AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
			graph.rollUpOverTimeAndVisibility(rollUpOverTimeAndVisibility);

			// Read seeds
			logger.info("Reading ranges from file " + seedsFile);
			Set<TypeValueRange> ranges = new HashSet<TypeValueRange>();
			BufferedReader reader = new BufferedReader(new FileReader(new File(seedsFile)));

			String line;
			int count = 0;
			while ( (line = reader.readLine()) != null ) {
				String[] splits = line.split("\\|");
				if (splits.length != 4) {
					logger.error("Error parsing line: " + line);
				} else {
					ranges.add(new TypeValueRange(splits[0], splits[1], splits[2], splits[3]));
					count++;
				}
			}
			reader.close();
			logger.info("Lines read = " + count);
			logger.info("Number of distinct ranges read = " + ranges.size());

			// Create parameters from seeds
			TypeValueRangeSet rangesSet = new TypeValueRangeSet();
			rangesSet.setAccumuloGraph(graph);
			rangesSet.setTypeValueRanges(ranges);

			// Create analytic
			GetElementsFromRanges getElements = new GetElementsFromRanges();
			try {
				getElements.setParameters(rangesSet);
			} catch (IllegalArgumentException e) {
				logger.error("Invalid Parameters: " + e.getMessage());
				return;
			}

			// Get results
			for (GraphElementWithStatistics gews : getElements.getResult().getResults()) {
				System.out.println(gews);
			}
			getElements.close();

		} catch (IOException e) {
			logger.error("IOException " + e.getMessage());
		} catch (AccumuloException e) {
			logger.error("AccumuloException " + e.getMessage());
		} catch (AccumuloSecurityException e) {
			logger.error("AccumuloSecurityException " + e.getMessage());
		}
	}

}
