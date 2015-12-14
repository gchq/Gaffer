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
package gaffer.accumulo.retrievers.impl;

import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.TableUtils;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.accumulo.predicate.RawGraphElementWithStatisticsPredicate;
import gaffer.predicate.Predicate;
import gaffer.predicate.graph.impl.IsEdgePredicate;
import gaffer.predicate.graph.impl.IsEntityPredicate;

import gaffer.statistics.transform.StatisticsTransform;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Utility methods for creating a {@link BatchScanner}, used by the retriever classes.
 */
public class RetrieverUtilities {

	private RetrieverUtilities() {}

	static BatchScanner getScanner(Connector connector, Authorizations auths, String tableName,
			int threadsForBatchScanner, boolean useRollUpOverTimeAndVisibilityIterator,
			Predicate<RawGraphElementWithStatistics> filterPredicate, StatisticsTransform statisticsTransform)
			throws TableNotFoundException {
		BatchScanner scanner = connector.createBatchScanner(tableName, auths, threadsForBatchScanner);
		if (useRollUpOverTimeAndVisibilityIterator) {
			TableUtils.addRollUpOverTimeAndVisibilityIteratorToScanner(scanner);
		}
		if (filterPredicate != null) {
			TableUtils.addPreRollUpFilterIteratorToScanner(scanner, filterPredicate);
		}
		if (statisticsTransform != null) {
			TableUtils.addStatisticsTransformToScanner(scanner, statisticsTransform);
		}
		return scanner;
	}

	static BatchScanner getScanner(Connector connector, Authorizations auths, String tableName,
			int threadsForBatchScanner, boolean useRollUpOverTimeAndVisibilityIterator,
			Predicate<RawGraphElementWithStatistics> filterPredicate, StatisticsTransform statisticsTransform,
			boolean returnEntities, boolean returnEdges) throws TableNotFoundException {
		if (returnEntities && !returnEdges) {
			filterPredicate = AccumuloBackedGraph.andPredicates(filterPredicate,
					new RawGraphElementWithStatisticsPredicate(new IsEntityPredicate()));
		} else if (!returnEntities && returnEdges) {
			filterPredicate = AccumuloBackedGraph.andPredicates(filterPredicate,
					new RawGraphElementWithStatisticsPredicate(new IsEdgePredicate()));
		}
		BatchScanner scanner = getScanner(connector, auths, tableName,
				threadsForBatchScanner, useRollUpOverTimeAndVisibilityIterator,
				filterPredicate, statisticsTransform);
		return scanner;
	}
	
}
