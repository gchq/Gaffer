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

import gaffer.CloseableIterable;
import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.graph.TypeValue;
import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.Predicate;
import gaffer.statistics.SetOfStatistics;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import gaffer.statistics.transform.StatisticsTransform;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

/**
 * This allows queries for all data about the provided {@link TypeValue}s. It batches up the
 * provided seeds into groups of size appropriate to give to one {@link BatchScanner}, when all
 * results for one scanner have been consumed it creates the next and provides results from that.
 * This means that the user does not have to think about batching seeds themselves. The results
 * are provided as {@link GraphElementWithStatistics} which means that the user does not have
 * to think about Accumulo {@link Key}s and {@link Value}s.
 * 
 * It allows a view on the data to be set: this includes specifying whether entities or edges or both
 * are wanted; the start and end time windows; the required summary types and subtypes; and whether
 * elements should be rolled up over time and security label; etc.
 */
public class GraphElementWithStatisticsRetrieverFromEntities implements CloseableIterable<GraphElementWithStatistics> {

	// Parameters specifying connection to Accumulo
	private Connector connector;
	private Authorizations auths;
	private String tableName;
	private int maxEntriesForBatchScanner;
	private int threadsForBatchScanner;
	
	// View on data
	private boolean useRollUpOverTimeAndVisibilityIterator;
	private Predicate<RawGraphElementWithStatistics> filterPredicate;
	private StatisticsTransform statisticsTransform;
	private Transform postRollUpTransform;
	private boolean returnEntities;
	private boolean returnEdges;

	// TypeValues to retrieve data for
	private Iterable<TypeValue> entities;
	private boolean someEntitiesProvided;
	
	// Iterator
	private GraphElementWithStatisticsIterator graphElementWithStatisticsIterator = null;
	
	public GraphElementWithStatisticsRetrieverFromEntities(Connector connector, Authorizations auths, String tableName,
			int maxEntriesForBatchScanner, int threadsForBatchScanner,
			boolean useRollUpOverTimeAndVisibilityIterator,
			Predicate<RawGraphElementWithStatistics> filterPredicate,
			StatisticsTransform statisticsTransform,
			Transform postRollUpTransform,
			boolean returnEntities, boolean returnEdges,
			Iterable<TypeValue> entities) {
		this.connector = connector;
		this.auths = auths;
		this.tableName = tableName;
		this.maxEntriesForBatchScanner = maxEntriesForBatchScanner;
		this.threadsForBatchScanner = threadsForBatchScanner;
		this.useRollUpOverTimeAndVisibilityIterator = useRollUpOverTimeAndVisibilityIterator;
		this.filterPredicate = filterPredicate;
		this.statisticsTransform = statisticsTransform;
		this.postRollUpTransform = postRollUpTransform;
		this.returnEntities = returnEntities;
		this.returnEdges = returnEdges;
		this.entities = entities;
		this.someEntitiesProvided = this.entities.iterator().hasNext();
	}
	
	@Override
	public Iterator<GraphElementWithStatistics> iterator() {
		if (!someEntitiesProvided) {
			return Collections.emptyIterator();
		}
		graphElementWithStatisticsIterator = new GraphElementWithStatisticsIterator();
		return graphElementWithStatisticsIterator;
	}

	public void close() {
		if (graphElementWithStatisticsIterator != null) {
			graphElementWithStatisticsIterator.close();
		}
	}
	
	private class GraphElementWithStatisticsIterator implements Iterator<GraphElementWithStatistics> {

		private BatchScanner scanner;
		private Iterator<TypeValue> entitiesIterator;
		private Iterator<Entry<Key,Value>> scannerIterator;
		private int count;
		
		GraphElementWithStatisticsIterator() {
			entitiesIterator = entities.iterator();
			
			this.count = 0;
			Set<Range> ranges = new HashSet<Range>();
			while (this.entitiesIterator.hasNext() && count < maxEntriesForBatchScanner) {
				TypeValue typeValue = this.entitiesIterator.next();
				count++;
				// Get key and use to create appropriate range
				// Note that no need to apply EntityOrEdgeOnlyFilterIterator as the ranges take care of that
				Range range = ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue(), returnEntities, returnEdges);
				ranges.add(range);
			}
			
			try {
				scanner = RetrieverUtilities.getScanner(connector, auths, tableName,
						threadsForBatchScanner, useRollUpOverTimeAndVisibilityIterator,
						filterPredicate, statisticsTransform);
				scanner.setRanges(ranges);
				scannerIterator = scanner.iterator();
			} catch (TableNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public boolean hasNext() {
			// If current scanner has next then return true.
			if (scannerIterator.hasNext()) {
				return true;
			}
			// If current scanner is spent then go back to the iterator
			// through the provided entities, and see if there are more.
			// If so create the next scanner, if there are no more entities
			// then return false.
			while (entitiesIterator.hasNext() && !scannerIterator.hasNext()) {
				this.count = 0;
				Set<Range> ranges = new HashSet<Range>();
				
				while (this.entitiesIterator.hasNext() && this.count < maxEntriesForBatchScanner) {
					TypeValue typeValue = this.entitiesIterator.next();
					this.count++;
					// Get key and use to create appropriate range
					Range range = ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue(), returnEntities, returnEdges);
					ranges.add(range);
				}
				try {
					scanner.close();
					scanner = RetrieverUtilities.getScanner(connector, auths, tableName,
							threadsForBatchScanner, useRollUpOverTimeAndVisibilityIterator,
							filterPredicate, statisticsTransform);
					scanner.setRanges(ranges);
					scannerIterator = scanner.iterator();
				} catch (TableNotFoundException e) {
					throw new RuntimeException(e);
				}
			}
			if (!scannerIterator.hasNext()) {
				scanner.close();
			}
			return scannerIterator.hasNext();
		}

		@Override
		public GraphElementWithStatistics next() {
			Entry<Key,Value> entry = scannerIterator.next();
			try {
				GraphElement ge = ConversionUtils.getGraphElementFromKey(entry.getKey());
				SetOfStatistics setOfStatistics = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
				GraphElementWithStatistics gews = new GraphElementWithStatistics(ge, setOfStatistics);
				if (postRollUpTransform == null) {
					return gews;
				}
				return postRollUpTransform.transform(gews);
			} catch (IOException e) {
				return null;
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Can't remove elements from a graph element iterator");
		}
		
		public void close() {
			if (scanner != null) {
				scanner.close();
			}
		}
		
	}

}
