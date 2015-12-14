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
import gaffer.Pair;
import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.graph.Edge;
import gaffer.graph.TypeValue;
import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.EdgeWithStatistics;
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
 * This allows queries for all data about the provided {@link Pair} of {@link TypeValue}s. It batches
 * up the provided seeds into groups of size appropriate to give to one {@link BatchScanner}, when all
 * results for one scanner have been consumed it creates the next and provides results from that.
 * This means that the user does not have to think about batching seeds themselves. The results
 * are provided as {@link EdgeWithStatistics} which means that the user does not have
 * to think about Accumulo {@link Key}s and {@link Value}s.
 * 
 * It allows a view on the data to be set: the start and end time windows; the required summary types and
 * subtypes; and whether elements should be rolled up over time and security label; etc.
 */
public class EdgeWithStatisticsRetrieverFromPairs implements CloseableIterable<EdgeWithStatistics> {

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

	// TypeValues to retrieve data for
	private Iterable<Pair<TypeValue>> typeValuePairs;
	private boolean somePairsProvided;
	
	// Iterator
	private EdgeWithStatisticsIterator edgeWithStatisticsIterator = null;
	
	public EdgeWithStatisticsRetrieverFromPairs(Connector connector, Authorizations auths, String tableName,
			int maxEntriesForBatchScanner, int threadsForBatchScanner,
			boolean useRollUpOverTimeAndVisibilityIterator,
			Predicate<RawGraphElementWithStatistics> filterPredicate,
			StatisticsTransform statisticsTransform,
			Transform postRollUpTransform,
			Iterable<Pair<TypeValue>> typeValuePairs) {
		this.connector = connector;
		this.auths = auths;
		this.tableName = tableName;
		this.maxEntriesForBatchScanner = maxEntriesForBatchScanner;
		this.threadsForBatchScanner = threadsForBatchScanner;
		this.useRollUpOverTimeAndVisibilityIterator = useRollUpOverTimeAndVisibilityIterator;
		this.filterPredicate = filterPredicate;
		this.statisticsTransform = statisticsTransform;
		this.postRollUpTransform = postRollUpTransform;
		this.typeValuePairs = typeValuePairs;
		this.somePairsProvided = this.typeValuePairs.iterator().hasNext();
	}
	
	@Override
	public Iterator<EdgeWithStatistics> iterator() {
		if (!somePairsProvided) {
			return Collections.emptyIterator();
		}
		edgeWithStatisticsIterator = new EdgeWithStatisticsIterator();
		return edgeWithStatisticsIterator;
	}

	@Override
	public void close() {
		if (edgeWithStatisticsIterator != null) {
			edgeWithStatisticsIterator.close();
		}
	}
	
	private class EdgeWithStatisticsIterator implements Iterator<EdgeWithStatistics> {

		private BatchScanner scanner;
		private Iterator<Pair<TypeValue>> typeValuePairsIterator;
		private Iterator<Entry<Key,Value>> scannerIterator;
		private int count;
		
		EdgeWithStatisticsIterator() {
			typeValuePairsIterator = typeValuePairs.iterator();
			
			this.count = 0;
			Set<Range> ranges = new HashSet<Range>();
			while (this.typeValuePairsIterator.hasNext() && count < maxEntriesForBatchScanner) {
				Pair<TypeValue> typeValuePair = this.typeValuePairsIterator.next();
				count++;
				// Get key and use to create appropriate range
				Range range = ConversionUtils.getRangeFromPairOfTypesAndValues(typeValuePair.getFirst().getType(),
						typeValuePair.getFirst().getValue(), typeValuePair.getSecond().getType(),
						typeValuePair.getSecond().getValue());
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
			while (typeValuePairsIterator.hasNext() && !scannerIterator.hasNext()) {
				this.count = 0;
				Set<Range> ranges = new HashSet<Range>();
				
				while (this.typeValuePairsIterator.hasNext() && this.count < maxEntriesForBatchScanner) {
					Pair<TypeValue> typeValuePair = this.typeValuePairsIterator.next();
					this.count++;
					// Get key and use to create appropriate range
					Range range = ConversionUtils.getRangeFromPairOfTypesAndValues(typeValuePair.getFirst().getType(),
							typeValuePair.getFirst().getValue(), typeValuePair.getSecond().getType(),
							typeValuePair.getSecond().getValue());
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
		public EdgeWithStatistics next() {
			Entry<Key,Value> entry = scannerIterator.next();
			try {
				SetOfStatistics setOfStatistics = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
				if (postRollUpTransform == null) {
					Edge edge = ConversionUtils.getEdgeFromKey(entry.getKey());
					return new EdgeWithStatistics(edge, setOfStatistics);
				}
				GraphElement graphElement = ConversionUtils.getGraphElementFromKey(entry.getKey());
				GraphElementWithStatistics transformed = postRollUpTransform.transform(new GraphElementWithStatistics(graphElement, setOfStatistics));
				// Sanity check that an Edge hasn't been transformed into an Entity
				if (transformed.isEntity()) {
					return null;
				}
				return new EdgeWithStatistics(transformed.getGraphElement().getEdge(), transformed.getSetOfStatistics());
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
