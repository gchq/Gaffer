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
package gaffer.accumulo;

import gaffer.CloseableIterable;
import gaffer.GraphAccessException;
import gaffer.Pair;
import gaffer.accumulo.inputformat.ElementInputFormat;
import gaffer.accumulo.insert.InsertIntoGraphUtilities;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.accumulo.predicate.RawGraphElementWithStatisticsPredicate;
import gaffer.accumulo.predicate.impl.IncomingEdgePredicate;
import gaffer.accumulo.predicate.impl.OtherEndOfEdgePredicate;
import gaffer.accumulo.predicate.impl.OutgoingEdgePredicate;
import gaffer.accumulo.predicate.impl.ReturnEdgesOnlyOncePredicate;
import gaffer.accumulo.retrievers.impl.*;
import gaffer.accumulo.utils.AccumuloConfig;
import gaffer.graph.*;
import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.EdgeWithStatistics;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.CombinedPredicates;
import gaffer.predicate.Predicate;
import gaffer.predicate.graph.impl.DirectedEdgePredicate;
import gaffer.predicate.graph.impl.IsEdgePredicate;
import gaffer.predicate.graph.impl.IsEntityPredicate;
import gaffer.predicate.graph.impl.UndirectedEdgePredicate;
import gaffer.predicate.summarytype.SummaryTypePredicate;
import gaffer.predicate.summarytype.impl.SummaryTypeAndSubTypeInSetPredicate;
import gaffer.predicate.summarytype.impl.SummaryTypeInSetPredicate;
import gaffer.predicate.time.TimePredicate;
import gaffer.predicate.time.impl.AfterTimePredicate;
import gaffer.predicate.time.impl.BeforeTimePredicate;
import gaffer.predicate.time.impl.TimeWindowPredicate;
import gaffer.predicate.typevalue.TypeValuePredicate;
import gaffer.predicate.typevalue.impl.TypeInSetPredicate;
import gaffer.predicate.typevalue.impl.ValueRegularExpressionPredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.Statistic;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import gaffer.statistics.transform.StatisticsTransform;
import gaffer.statistics.transform.impl.StatisticsRemoverByName;
import gaffer.utils.WritableToStringConverter;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.bloom.BloomFilter;

/**
 * Represents a view of a Gaffer Accumulo table as a graph. Contains methods to retrieve
 * {@link GraphElement}s and {@link GraphElementWithStatistics} from the underlying
 * Accumulo instance, subject to the current view on the graph.
 * 
 * A view on the graph is defined by the following:
 * 	- The authorizations of the user that is connecting (or the user that we are connecting on
 * 		behalf of).
 * 	- The time period of interest - note that for a graph element to be returned its time window
 * 		must be a subset of the time period of interest, i.e. the start date of the element
 * 		must be the same as or after the start of the time period of interest and the end date
 * 		of the element must be the same as of before the end of the time period of interest.
 * 	- The summaryTypes and summarySubTypes of the {@link Entity}s and {@link Edge}s we wish to receive.
 * 	- Whether we wish to roll data up over time and visibility or not.
 *  - Whether we want to receive both entities and edges, or only entities, or only edges.
 * 
 * Also contains methods to allow new data to be added to the graph in near real-time.
 */
public class AccumuloBackedGraph implements QueryableGraph, UpdateableGraph {

	// Connection to Accumulo
	protected Connector connector;
	protected String tableName;
	protected Authorizations authorizations;
	// Parameters for queries
	protected int maxEntriesForBatchScanner;
	protected int threadsForBatchScanner;
	// Parameters for inserts
	protected long bufferSizeForBatchWriter;
	protected long timeOutForBatchWriter;
	protected int numThreadsForBatchWriter;
	// Parameters for Bloom filters (used in the method that gets edges within a set).
	protected int maxBloomFilterToPassToAnIterator; // Max size of Bloom filter to pass to an iterator
	protected double falsePositiveRateForBloomFilter; // Desired false positive rate for the Bloom filter
	protected int clientSideBloomFilterSize; // Size of the client side Bloom filter used for secondary testing when we
											// don't load all seeds into memory in the get edges within a set method.
	// View on the graph
	protected TimePredicate timePredicate;
	protected boolean rollUpOverTimeAndVisibility;
	protected boolean returnEntities;
	protected boolean returnEdges;
	protected SummaryTypePredicate summaryTypePredicate;
	protected boolean undirectedEdgesOnly;
	protected boolean directedEdgesOnly;
	protected boolean outgoingEdgesOnly;
	protected boolean incomingEdgesOnly;
	protected TypeValuePredicate otherEndOfEdgePredicate;
	protected Transform postRollUpTransform;
	protected Set<String> statisticsToKeepByName;
	protected Set<String> statisticsToRemoveByName;

	/**
	 * Sets up a connection to Gaffer data in Accumulo. It sets a default view:
	 * 	- Uses the authorizations of the user
	 * 	- The time period of interest is all of time
	 *  - Roll up over time and visibility is set to true
	 *  - Both entities and edges will be returned by queries
	 *  - All summary types will be returned
	 * 
	 * @param connector
	 * @param tableName
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public AccumuloBackedGraph(Connector connector, String tableName) throws AccumuloException, AccumuloSecurityException {
		this.connector = connector;
		this.tableName = tableName;
		this.authorizations = connector.securityOperations().getUserAuthorizations(connector.whoami());
		this.maxEntriesForBatchScanner = Constants.MAX_ENTRIES_FOR_BATCH_SCANNER;
		this.threadsForBatchScanner = Constants.THREADS_FOR_BATCH_SCANNER;
		this.bufferSizeForBatchWriter = Constants.MAX_BUFFER_SIZE_FOR_BATCH_WRITER;
		this.timeOutForBatchWriter = Constants.MAX_TIME_OUT_FOR_BATCH_WRITER;
		this.numThreadsForBatchWriter = Constants.NUM_THREADS_FOR_BATCH_WRITER;
		this.maxBloomFilterToPassToAnIterator = Constants.MAX_SIZE_BLOOM_FILTER;
		this.falsePositiveRateForBloomFilter = Constants.FALSE_POSITIVE_RATE_FOR_BLOOM_FILTER;
		this.clientSideBloomFilterSize = Constants.SIZE_CLIENT_SIDE_BLOOM_FILTER;
		this.timePredicate = null;
		this.rollUpOverTimeAndVisibility = true;
		this.returnEntities = true;
		this.returnEdges = true;
		this.summaryTypePredicate = null;
		this.undirectedEdgesOnly = false;
		this.directedEdgesOnly = false;
		this.outgoingEdgesOnly = false;
		this.incomingEdgesOnly = false;
		this.statisticsToKeepByName = null;
		this.statisticsToRemoveByName = null;
	}

	/**
	 * Returns all {@link GraphElement}s involving the given {@link TypeValue},
	 * subject to the view that's currently defined on the graph (i.e. the authorizations, the
	 * time window, the required summary types, whether we want entities and edges or just entities or
	 * just edges, and whether we want to roll up over time and visibility).
	 * 
	 * Note that the result is a {@link CloseableIterable} of {@link GraphElement}s so that the results
	 * can be consumed within a <code>for</code> loop. The retriever should be closed when finished with:
	 * {@code
	 * CloseableIterable<GraphElement> retriever = graph.getGraphElements(typeValue);
	 * for (GraphElement element : retriever) {
	 * 	 // do something with element
	 * }
	 * retriever.close();
	 * }
	 */
	@Override
	public CloseableIterable<GraphElement> getGraphElements(TypeValue typeValue) {
		return getGraphElements(Collections.singleton(typeValue));
	}

	/**
	 * Returns all {@link GraphElement}s involving the given {@link TypeValue}s from
	 * the provided {@link Iterable}, subject to the view that's currently defined on the graph
	 * (i.e. the authorizations, the time window, the required summary types, whether we want entities and
	 * edges or just entities or just edges, and whether we want to roll up over time and visibility).
	 * 
	 * Note that the result is a {@link CloseableIterable} of {@link GraphElement}s
	 * so that the results can be consumed within a <code>for</code> loop. The retriever should be closed
	 * when finished with:
	 * {@code
	 * CloseableIterable<GraphElement> retriever = graph.getGraphElements(typeValues);
	 * for (GraphElement element : retriever) {
	 * 	 // do something with element
	 * }
	 * retriever.close();
	 * }
	 */
	@Override
	public CloseableIterable<GraphElement> getGraphElements(Iterable<TypeValue> typeValues) {
		Predicate<RawGraphElementWithStatistics> predicate = andPredicates(createFilterPredicate(), createFilterPredicateForQuery());
		// As we only want to retrieve GraphElements then filter all out as soon as possible
		StatisticsTransform statisticsTransform = new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.KEEP, Collections.EMPTY_SET);
		return new GraphElementRetrieverFromEntities(connector, authorizations, tableName, maxEntriesForBatchScanner, threadsForBatchScanner,
				rollUpOverTimeAndVisibility, predicate, statisticsTransform, getPostRollUpTransform(), returnEntities, returnEdges, typeValues);
	}

	/**
	 * Returns all {@link GraphElementWithStatistics}s involving the given {@link TypeValue},
	 * subject to the view that's currently defined on the graph (i.e. the authorizations, the
	 * time window, the required summary types, whether we want entities and edges or just entities or
	 * just edges, and whether we want to roll up over time and visibility).
	 * 
	 * Note that the result is a {@link CloseableIterable} of {@link GraphElementWithStatistics} so that the
	 * results can be consumed within a <code>for</code> loop. The retriever should be closed when finished with:
	 * {@code
	 * CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(typeValues);
	 * for (GraphElementWithStatistics elementWithStats : retriever) {
	 * 	 // do something with elementWithStats
	 * }
	 * retriever.close();
	 * }
	 */
	@Override
	public CloseableIterable<GraphElementWithStatistics> getGraphElementsWithStatistics(TypeValue typeValue) {
		return getGraphElementsWithStatistics(Collections.singleton(typeValue));
	}

	/**
	 * Returns all {@link GraphElementWithStatistics} involving the given {@link TypeValue}s from
	 * the provided {@link Iterable}, subject to the view that's currently defined on the graph
	 * (i.e. the authorizations, the time window, the required summary types, whether we want entities and
	 * edges or just entities or just edges, and whether we want to roll up over time and visibility).
	 * 
	 * Note that the result is an {@link CloseableIterable} of {@link GraphElementWithStatistics} so that the
	 * results can be consumed within a <code>for</code> loop. The retriever should be closed when finished with:
	 * {@code
	 * CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(typeValues);
	 * for (GraphElementWithStatistics elementWithStats : retriever) {
	 * 	 // do something with elementWithStats
	 * }
	 * retriever.close();
	 * }
	 */
	@Override
	public CloseableIterable<GraphElementWithStatistics> getGraphElementsWithStatistics(Iterable<TypeValue> typeValues) {
		Predicate<RawGraphElementWithStatistics> predicate = andPredicates(createFilterPredicate(), createFilterPredicateForQuery());
		StatisticsTransform statisticsTransform = calculateStatisticsTransform();
		return new GraphElementWithStatisticsRetrieverFromEntities(connector, authorizations, tableName, maxEntriesForBatchScanner, threadsForBatchScanner,
				rollUpOverTimeAndVisibility, predicate, statisticsTransform, getPostRollUpTransform(), returnEntities, returnEdges, typeValues);
	}

	/**
	 * Returns all {@link EdgeWithStatistics} involving the given {@link Pair} of {@link TypeValue}s, i.e. every edge
	 * in which the unordered pair of the source and destination match the provided pair of {@link TypeValue}s.
	 * Note that it is not necessary to worry about the order of the entities in the pair, i.e. if you ask for
	 * the pair (typeA|valueA, typeB|valueB) then a directed edge from typeB|valueB to typeA|valueA will still
	 * be returned.
	 *
	 * This is subject to the view that's currently defined on the graph (i.e. the authorizations, the time window,
	 * the required summary types, and whether we want to roll up over time and visibility). Note that the parameter
	 * set on the graph which specifies whether we want to see just {@link Entity}s, just {@link Edge}s or both
	 * is ignored by this method.
	 *
	 * Note that the result is a {@link CloseableIterable} of {@link EdgeWithStatistics} so that the results
	 * can be consumed within a <code>for</code> loop. The retriever should be closed when finished with:
	 * {@code
	 * CloseableIterable<EdgeWithStatistics> retriever = graph.getEdgesWithStatisticsFromPair(typeValuePair);
	 * for (EdgeWithStatistics edgeWithStats : retriever) {
	 * 	 // do something with edgeWithStats
	 * }
	 * retriever.close();
	 * }
	 */
	@Override
	public CloseableIterable<EdgeWithStatistics> getEdgesWithStatisticsFromPair(Pair<TypeValue> typeValuePair) {
		return getEdgesWithStatisticsFromPairs(Collections.singleton(typeValuePair));
	}

	/**
	 * Returns all {@link EdgeWithStatistics} involving the given {@link Pair} of {@link TypeValue}s, i.e. every edge
	 * in which the unordered pair of the source and destination match the provided pair of {@link TypeValue}s.
	 * Note that it is not necessary to worry about the order of the entities in the pair, i.e. if you ask for
	 * the pair (typeA|valueA, typeB|valueB) then a directed edge from typeB|valueB to typeA|valueA will still
	 * be returned.
	 * 
	 * This is subject to the view that's currently defined on the graph (i.e. the authorizations, the time window,
	 * the required summary types, and whether we want to roll up over time and visibility). Note that the parameter
	 * set on the graph which specifies whether we want to see just {@link Entity}s, just {@link Edge}s or both
	 * is ignored by this method.
	 * 
	 * Note that the result is a {@link CloseableIterable} of {@link EdgeWithStatistics} so that the results
	 * can be consumed within a <code>for</code> loop. The retriever should be closed when finished with:
	 * {@code
	 * CloseableIterable<EdgeWithStatistics> retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
	 * for (EdgeWithStatistics edgeWithStats : retriever) {
	 * 	 // do something with edgeWithStats
	 * }
	 * retriever.close();
	 * }
	 */
	@Override
	public CloseableIterable<EdgeWithStatistics> getEdgesWithStatisticsFromPairs(Iterable<Pair<TypeValue>> typeValuePairs) {
		Predicate<RawGraphElementWithStatistics> predicate = andPredicates(createFilterPredicate(), createFilterPredicateForQuery());
		StatisticsTransform statisticsTransform = calculateStatisticsTransform();
		return new EdgeWithStatisticsRetrieverFromPairs(connector, authorizations, tableName, maxEntriesForBatchScanner, threadsForBatchScanner,
				rollUpOverTimeAndVisibility, predicate, statisticsTransform, getPostRollUpTransform(), typeValuePairs);
	}
	
	/**
	 * Returns all {@link GraphElementWithStatistics} from within the given {@link TypeValueRange}s,
	 * i.e. for which the type-value (in the case of an {@link Entity}) or one of the two type-values
	 * (in the case of an {@link Edge}) is within the range. This is subject to the view that's currently
	 * defined on the graph (i.e. the authorizations, the time window, the required summary types, whether we
	 * want entities and edges or just entities or just edges, and whether we want to roll up over time and
	 * visibility).
	 * 
	 * Note that the result is a {@link CloseableIterable} of {@link GraphElementWithStatistics} so that the results
	 * can be consumed within a <code>for</code> loop. The retriever should be closed when finished with:
	 * {@code
	 * CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsFromRanges(typeValueRanges);
	 * for (GraphElementWithStatistics elementWithStats : retriever) {
	 * 	 // do something with elementWithStats
	 * }
	 * retriever.close();
	 * }
	 */
	public CloseableIterable<GraphElementWithStatistics> getGraphElementsWithStatisticsFromRanges(Iterable<TypeValueRange> typeValueRanges) {
		Predicate<RawGraphElementWithStatistics> predicate = andPredicates(createFilterPredicate(), createFilterPredicateForQuery());
		StatisticsTransform statisticsTransform = calculateStatisticsTransform();
		return new GraphElementWithStatisticsRetrieverFromRanges(connector, authorizations, tableName, maxEntriesForBatchScanner, threadsForBatchScanner,
				rollUpOverTimeAndVisibility, predicate, statisticsTransform, getPostRollUpTransform(), returnEntities, returnEdges, typeValueRanges);
	}

	/**
	 * Returns all {@link Edge}s which have both ends in the provided set of {@link TypeValue}s. Note this will also
	 * return any {@link Entity}s for the provided {@link TypeValue}s as well (unless {@link #setReturnEdgesOnly()}
	 * has been called).
	 *
	 * This is done by passing a Bloom filter of the provided {@link TypeValue}s to the filtering iterator. This is
	 * used to avoid returning all edges out of each {@link TypeValue} to the client - only edges where the second
	 * node (i.e. the non-query node) matches the Bloom filter are returned. This reduces the amount of data returned
	 * from Accumulo's tablet servers.
	 *
	 * If the <code>loadIntoMemory</code> parameter is <code>true</code> then all the provided {@link TypeValue}s
	 * are loaded into memory (in the client JVM) and this set is used to filter out any false positives that may be
	 * returned from the tablet servers. It is guaranteed that the results returned to the user will be correct, i.e.
	 * no false positives.
	 *
	 * If the <code>loadIntoMemory</code> parameter is <code>false</code> then a secondary {@link BloomFilter} is used
	 * client-side in an attempt to filter out any false positives from the {@link BloomFilter} used by the filtering
	 * iterator. It is not guaranteed that the results returned to the the user will contain only edges within the set
	 * --- there is a very small probability that some false positives will make it through to the user.
	 *
	 * NB: The options {@link #setOutgoingEdgesOnly()} and {@link #setIncomingEdgesOnly()} are ignored when this
	 * method is called.
	 *
	 * NB: This method may return {@link Edge}s twice.
	 *
	 * Note that the result is a {@link CloseableIterable} of {@link GraphElementWithStatistics} so that the results
	 * can be consumed within a <code>for</code> loop. The retriever should be closed when finished with:
	 * {@code
	 * CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsWithinSet(typeValues, boolean);
	 * for (GraphElementWithStatistics elementWithStats : retriever) {
	 * 	 // do something with elementWithStats
	 * }
	 * retriever.close();
	 * }
	 *
	 * @param typeValues
	 * @return
	 */
	public CloseableIterable<GraphElementWithStatistics> getGraphElementsWithStatisticsWithinSet(Iterable<TypeValue> typeValues, boolean loadIntoMemory) {
		// NB: Deliberately do not include the predicate from createFilterPredicateForQuery() in the filter predicate
		// method here as want to ignore the outgoing edges and incoming edges only options.
		Predicate<RawGraphElementWithStatistics> predicate = createFilterPredicate();
		StatisticsTransform statisticsTransform = calculateStatisticsTransform();
		return new GraphElementWithStatisticsWithinSetRetriever(connector, authorizations, tableName, maxEntriesForBatchScanner,
				threadsForBatchScanner, maxBloomFilterToPassToAnIterator, falsePositiveRateForBloomFilter, clientSideBloomFilterSize,
				rollUpOverTimeAndVisibility, predicate, statisticsTransform,
				getPostRollUpTransform(), returnEntities, returnEdges, typeValues, loadIntoMemory);
	}

	/**
	 * Given two {@link Iterable}s of {@link TypeValue}s A and B, this returns all edges for which one end is in
	 * set A and the other is in set B. Note this will also return any {@link Entity}s for {@link TypeValue}s in
	 * set A as well (unless {@link #setReturnEdgesOnly()} has been called). {@link Entity}s for {@link TypeValue}s
	 * in set B are not returned.
	 *
	 * This is done by querying for the {@link TypeValue}s in set A and passing a Bloom filter of the
	 * {@link TypeValue}s in B to the filtering iterator. This is used to avoid returning all edges out of each
	 * {@link TypeValue} to the client - only edges where the second node (i.e. the non-query node) matches the
	 * Bloom filter are returned. This reduces the amount of data returned from Accumulo's tablet servers.
	 *
	 * If the <code>loadIntoMemory</code> parameter is <code>true</code> then all the provided {@link TypeValue}s
	 * are loaded into memory (in the client JVM) and this set is used to filter out any false positives that may be
	 * returned from the tablet servers. It is guaranteed that the results returned to the user will be correct, i.e.
	 * no false positives.
	 *
	 * If the <code>loadIntoMemory</code> parameter is <code>false</code> then a secondary {@link BloomFilter} is used
	 * client-side in an attempt to filter out any false positives from the {@link BloomFilter} used by the filtering
	 * iterator. It is not guaranteed that the results returned to the the user will contain only edges within the set
	 * --- there is a very small probability that some false positives will make it through to the user.
	 *
	 * Note that it is best for the smallest set of {@link TypeValue}s to be set A, as this will cause the least
	 * number of queries to Accumulo. The sets provided are not simply switched by Gaffer as {@link Entity}s for
	 * set A are returned to the user.
	 *
	 * NB: The options {@link #setOutgoingEdgesOnly()} and {@link #setIncomingEdgesOnly()} have the following meanings
	 * when this method is called. If the outgoing edges only option is set then only edges from set A to set B will
	 * be returned. If the incoming edges only option is set then only edges from set B to set A will be returned.
	 *
	 * NB: This method may return {@link Edge}s twice.
	 *
	 * Note that the result is a {@link CloseableIterable} of {@link GraphElementWithStatistics} so that the results
	 * can be consumed within a <code>for</code> loop. The retriever should be closed when finished with:
	 * {@code
	 * CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsBetweenSets(typeValuesA, typeValuesB, boolean);
	 * for (GraphElementWithStatistics elementWithStats : retriever) {
	 * 	 // do something with elementWithStats
	 * }
	 * retriever.close();
	 * }
	 *
	 * @param typeValuesA
	 * @param typeValuesB
	 * @param loadIntoMemory
	 * @return
	 */
	public CloseableIterable<GraphElementWithStatistics> getGraphElementsWithStatisticsBetweenSets(Iterable<TypeValue> typeValuesA,
			 Iterable<TypeValue> typeValuesB, boolean loadIntoMemory) {
		Predicate<RawGraphElementWithStatistics> predicate = andPredicates(createFilterPredicate(), createFilterPredicateForQuery());
		StatisticsTransform statisticsTransform = calculateStatisticsTransform();
		return new GraphElementWithStatisticsBetweenSetsRetriever(connector, authorizations, tableName, maxEntriesForBatchScanner,
				threadsForBatchScanner, maxBloomFilterToPassToAnIterator, falsePositiveRateForBloomFilter, clientSideBloomFilterSize,
				rollUpOverTimeAndVisibility, predicate, statisticsTransform,
				getPostRollUpTransform(), returnEntities, returnEdges, typeValuesA, typeValuesB, loadIntoMemory);
	}

	/**
	 * Set the time window of interest. For a {@link GraphElement} to be returned from the graph,
	 * its time window must be a subset of the time period of interest specified by calling this
	 * method, i.e. the start date of the element must be the same as or after the start of the time
	 * period of interest and the end date of the element must be the same as or before the end of
	 * the time period of interest.
	 */
	@Override
	public void setTimeWindow(Date startTimeWindow, Date endTimeWindow) {
		if (startTimeWindow == null || endTimeWindow == null || startTimeWindow.after(endTimeWindow)) {
			throw new IllegalArgumentException("Both startTimeWindow and endTimeWindow must not be null and the start must be before (or equal to) the end.");
		}
		this.timePredicate = new TimeWindowPredicate(startTimeWindow, endTimeWindow);
	}

	/**
	 * Set the time window of interest to be everything after the given {@link Date}. For a
	 * {@link GraphElement} to be returned from the graph, the start of its time window must
	 * be the same as the given {@link Date} or after it.
	 */
	@Override
	public void setTimeWindowToEverythingAfter(Date startTimeWindow) {
		if (startTimeWindow == null) {
			throw new IllegalArgumentException("The startTimeWindow must not be null.");
		}
		this.timePredicate = new AfterTimePredicate(startTimeWindow);
	}

	/**
	 * Set the time window of interest to be everything before the given {@link Date}. For a
	 * {@link GraphElement} to be returned from the graph, the end of its time window must
	 * be the same as the given {@link Date} or before it.
	 */
	@Override
	public void setTimeWindowToEverythingBefore(Date endTimeWindow) {
		if (endTimeWindow == null) {
			throw new IllegalArgumentException("The endTimeWindow must not be null.");
		}
		this.timePredicate = new BeforeTimePredicate(endTimeWindow);
	}

	/**
	 * Sets a predicate to determine whether {@link GraphElement}s are wanted based on their
	 * start and end dates. This is applied before roll-up.
	 *
	 * Calling this method cancels any previous calls to either {@link #setTimeWindow(Date, Date)},
	 * {@link #setTimeWindowToEverythingAfter(Date)} or {@link #setTimeWindowToEverythingBefore(Date)}.
	 *
	 * It is possible to use your own predicates (i.e. predicates which are not part
	 * of Gaffer itself), but this requires the class to be installed on all of Accumulo's tablet
	 * servers' classpaths - see your administrator.
	 *
	 * @param timePredicate
	 */
	@Override
	public void setTimePredicate(TimePredicate timePredicate) {
		try {
			if (!ClassLoadTestCache.checkLoad(connector, tableName, timePredicate.getClass().getName(), TimePredicate.class.getName())) {
				throw new IllegalArgumentException("This class is not on Accumulo's classpath (contact your system administrator).");
			}
		} catch (AccumuloSecurityException e) {
			throw new IllegalArgumentException("Exception setting time window predicate: " + e);
		} catch (AccumuloException e) {
			throw new IllegalArgumentException("Exception setting time window predicate: " + e);
		} catch (TableNotFoundException e) {
			throw new IllegalArgumentException("Exception setting time window predicate: " + e);
		}
		this.timePredicate = timePredicate;
	}

	/**
	 * Sets the time window of interest to be all time. This is used to reset the view on the graph
	 * after previous calls to {@link #setTimeWindow}, {@link #setTimeWindowToEverythingBefore} or
	 * {@link #setTimeWindowToEverythingAfter}.
	 */
	@Override
	public void setTimeWindowToAllTime() {
		this.timePredicate = null;
	}
	
	/**
	 * Sets the {@link Authorizations} with which the graph will be queried. This allows a processing
	 * user to run queries on behalf of a user with a certain set of permissions. It also allows
	 * users to restrict the data they see.
	 */
	public void setAuthorizations(Authorizations authorizations) {
		this.authorizations = authorizations;
	}

	/**
	 * Sets the summary types and sub types that are required. This is done by creating a
	 * {@link SummaryTypePredicate} - only {@link GraphElement}s that match the supplied
	 * {@link SummaryTypePredicate} will be returned. Note that there are other methods
	 * that are easier to use if the requirement is simple, e.g. {@link #setSummaryTypes} and
	 * {@link #setSummaryTypesAndSubTypes}.
	 * 
	 * Calling this method cancels any previous calls to either {@link #setSummaryTypes(Set)}
	 * or {@link #setSummaryTypesAndSubTypes(Set)}.
	 *
	 * See the package {@link gaffer.predicate.summarytype.impl} for possible
	 * predicates. It is possible to use your own predicates (i.e. predicates which are not part
	 * of Gaffer itself), but this requires the class to be installed on all of Accumulo's tablet
	 * servers' classpaths - see your administrator.
	 *
	 * @param summaryTypePredicate
	 */
	public void setSummaryTypePredicate(SummaryTypePredicate summaryTypePredicate) {
		try {
			if (!ClassLoadTestCache.checkLoad(connector, tableName, summaryTypePredicate.getClass().getName(), SummaryTypePredicate.class.getName())) {
				throw new IllegalArgumentException("This class is not on Accumulo's classpath (contact your system administrator).");
			}
		} catch (AccumuloSecurityException e) {
			throw new IllegalArgumentException("Exception setting summary type predicate: " + e);
		} catch (AccumuloException e) {
			throw new IllegalArgumentException("Exception setting summary type predicate: " + e);
		} catch (TableNotFoundException e) {
			throw new IllegalArgumentException("Exception setting summary type predicate: " + e);
		}
		this.summaryTypePredicate = summaryTypePredicate;
	}
	
	/**
	 * Sets the summary types of data to return from queries - for an {@link Edge} to be returned
	 * the edge summary type must be one of the types specified by this method, for an {@link Entity}
	 * to be returned its summary type must be one of the types specified by this method.
	 * 
	 * If {@link #setSummaryTypesAndSubTypes} is called after this method is called then it will
	 * override the effect of this method, i.e. the user must choose whether to specify the
	 * types they want by type only (i.e. I want type X whatever the subtype) or by both type
	 * and subtype (I want type X with subtype Y only).
	 * 
	 * For more general filtering of summary types and subtypes use the method
	 * {@link #setSummaryTypePredicate(SummaryTypePredicate)}.
	 */
	@Override
	public void setSummaryTypes(Set<String> summaryTypes) {
		this.summaryTypePredicate = new SummaryTypeInSetPredicate(summaryTypes);
	}

	/**
	 * Sets the summary types of data to return from queries - for an {@link Entity} or {@link Edge}
	 * to be returned the summary type must be one of the types specified by this method.
	 *
	 * If {@link #setSummaryTypesAndSubTypes} is called after this method is called then it will
	 * override the effect of this method, i.e. the user must choose whether to specify the
	 * types they want by type only (i.e. I want summary type X whatever the subtype) or by both
	 * type and subtype (I want summary type X with subtype Y only).
	 *
	 * For more general filtering of summary types and subtypes use the method
	 * {@link #setSummaryTypePredicate(SummaryTypePredicate)}.
	 */
	@Override
	public void setSummaryTypes(String... summaryTypes) {
		this.summaryTypePredicate = new SummaryTypeInSetPredicate(summaryTypes);
	}

	/**
	 * Sets the summary types of data to return from queries - for an {@link Edge} to be returned
	 * the edge summary type and subtype must be as specified by this method, for an {@link Entity}
	 * to be returned its summary type and subtype must be one of the type/subtype pairs
	 * specified by this method.
	 * 
	 * If {@link #setSummaryTypes} is called after this method is called then it will
	 * override the effect of this method, i.e. the user must choose whether to specify the
	 * types they want by type only (i.e. I want type X whatever the subtype) or by both type
	 * and subtype (I want type X with subtype Y only).
	 * 
	 * For more general filtering of summary types and subtypes use the method
	 * {@link #setSummaryTypePredicate(SummaryTypePredicate)}.
	 */
	@Override
	public void setSummaryTypesAndSubTypes(Set<Pair<String>> summaryTypesAndSubTypes) {
		this.summaryTypePredicate = new SummaryTypeAndSubTypeInSetPredicate(summaryTypesAndSubTypes);
	}

	/**
	 * Sets the summary types of data to return from queries - for an {@link Edge} to be returned
	 * the edge summary type and subtype must be as specified by this method, for an {@link Entity}
	 * to be returned its summary type and subtype must be one of the type/subtype pairs
	 * specified by this method.
	 *
	 * If {@link #setSummaryTypes} is called after this method is called then it will
	 * override the effect of this method, i.e. the user must choose whether to specify the
	 * types they want by type only (i.e. I want type X whatever the subtype) or by both type
	 * and subtype (I want type X with subtype Y only).
	 *
	 * For more general filtering of summary types and subtypes use the method
	 * {@link #setSummaryTypePredicate(SummaryTypePredicate)}.
	 */
	@Override
	public void setSummaryTypesAndSubTypes(Pair<String>... summaryTypesAndSubTypes) {
		this.summaryTypePredicate = new SummaryTypeAndSubTypeInSetPredicate(summaryTypesAndSubTypes);
	}

	/**
	 * Sets the summary types of data to return from queries to be all types, i.e. {@link Entity}s
	 * and {@link Edge}s will be returned whatever their summary type and subtype. Calling this
	 * method overrides previous calls to {@link #setSummaryTypes} or {@link #setSummaryTypesAndSubTypes}.
	 */
	@Override
	public void setReturnAllSummaryTypesAndSubTypes() {
		this.summaryTypePredicate = null;
	}
	
	/**
	 * Allows the user to specify that they only want {@link Entity}s to be returned by queries. This
	 * overrides any previous calls to {@link #setReturnEdgesOnly} or {@link #setReturnEntitiesAndEdges}.
	 */
	@Override
	public void setReturnEntitiesOnly() {
		this.returnEntities = true;
		this.returnEdges = false;
	}

	/**
	 * Allows the user to specify that they only want {@link Edge}s to be returned by queries. This
	 * overrides any previous calls to {@link #setReturnEntitiesOnly} or {@link #setReturnEntitiesAndEdges}.
	 */
	@Override
	public void setReturnEdgesOnly() {
		this.returnEntities = false;
		this.returnEdges = true;
	}

	/**
	 * Allows the user to specify that they want both {@link Entity}s and {@link Edge}s
	 * to be returned by queries. This overrides any previous calls to {@link #setReturnEntitiesOnly} or
	 * {@link #setReturnEdgesOnly}.
	 */
	@Override
	public void setReturnEntitiesAndEdges() {
		this.returnEntities = true;
		this.returnEdges = true;
	}

	/**
	 * Allows the user to specify that if there are any {@link Edge}s in their results
	 * then they only want to receive undirected ones. Note that setting this option does
	 * not cause all {@link Entity}s to be excluded from the results - if that is desired
	 * then {@link #setReturnEdgesOnly} should be called as well as calling this method.
	 *
	 * This overrides any previous calls to {@link #setDirectedEdgesOnly}.
	 */
	@Override
	public void setUndirectedEdgesOnly() {
		if (this.returnEdges == false) {
			throw new IllegalArgumentException("Cannot ask for only undirected edges when setReturnEntitiesOnly "
					+ "has been used to specify that only entities are required");
		}
		this.undirectedEdgesOnly = true;
		this.directedEdgesOnly = false;
	}

	/**
	 * Allows the user to specify that if there are any {@link Edge}s in their results
	 * then they only want to receive directed ones. Note that setting this option does
	 * not cause all {@link Entity}s to be excluded from the results - if that is desired
	 * then {@link #setReturnEdgesOnly} should be called as well as calling this method.
	 *
	 * This overrides any previous calls to {@link #setUndirectedEdgesOnly}.
	 */
	@Override
	public void setDirectedEdgesOnly() {
		if (this.returnEdges == false) {
			throw new IllegalArgumentException("Cannot ask for only undirected edges when setReturnEntitiesOnly "
					+ "has been used to specify that only entities are required");
		}
		this.undirectedEdgesOnly = false;
		this.directedEdgesOnly = true;
	}

	/**
	 * This cancels out any previous calls to either {@link #setDirectedEdgesOnly()} or {@link #setUndirectedEdgesOnly()}.
	 */
	@Override
	public void setUndirectedAndDirectedEdges() {
		if (this.returnEdges == false) {
			throw new IllegalArgumentException("Cannot ask for both directed and undirected edges when setReturnEntitiesOnly "
					+ "has been used to specify that only entities are required");
		}
		this.undirectedEdgesOnly = false;
		this.directedEdgesOnly = false;
	}

	/**
	 * Allows the user to specify that if there are any {@link Edge}s in their results
	 * then they only want edges which are out from the {@link TypeValue}s they queried
	 * for. An undirected {@link Edge} counts as out from both ends. If the user queries for
	 * a {@link TypeValueRange} then the edge must be outgoing from the {@link TypeValue}
	 * which is in the range.
	 *
	 * This overrides any previous calls to {@link #setIncomingEdgesOnly}.
	 *
	 * Note that calling this method does not cause only {@link Edge}s to be returned - {@link
	 * Entity}s can still be returned.
	 *
	 * For example, to cause only directed, outgoing edges from the query terms to be returned,
	 * then {@link #setReturnEdgesOnly}, {@link #setDirectedEdgesOnly} and {@link #setOutgoingEdgesOnly}
	 * should all be called.
	 */
	@Override
	public void setOutgoingEdgesOnly() {
		if (this.returnEdges == false) {
			throw new IllegalArgumentException("Cannot ask for only outgoing edges when setReturnEntitiesOnly "
					+ "has been used to specify that only entities are required");
		}
		this.outgoingEdgesOnly = true;
		this.incomingEdgesOnly = false;
	}

	/**
	 * Allows the user to specify that if there are any {@link Edge}s in their results
	 * then they only want edges which are in to the {@link TypeValue}s they queried
	 * for. An undirected {@link Edge} counts as being in to both ends. If the user queries for
	 * a {@link TypeValueRange} then the edge must be incoming to the {@link TypeValue}
	 * which is in the range.
	 *
	 * This overrides any previous calls to {@link #setOutgoingEdgesOnly}.
	 *
	 * Note that calling this method does not cause only {@link Edge}s to be returned - {@link
	 * Entity}s can still be returned.
	 *
	 * For example, to cause only directed, incoming edges to the query terms to be returned,
	 * then {@link #setReturnEdgesOnly}, {@link #setDirectedEdgesOnly} and {@link #setIncomingEdgesOnly}
	 * should all be called.
	 */
	@Override
	public void setIncomingEdgesOnly() {
		if (this.returnEdges == false) {
			throw new IllegalArgumentException("Cannot ask for only incoming edges when setReturnEntitiesOnly "
					+ "has been used to specify that only entities are required");
		}
		this.outgoingEdgesOnly = false;
		this.incomingEdgesOnly = true;
	}

	/**
	 * This cancels out any previous calls to either {@link #setOutgoingEdgesOnly} or {@link #setIncomingEdgesOnly}.
	 */
	@Override
	public void setOutgoingAndIncomingEdges() {
		this.outgoingEdgesOnly = false;
		this.incomingEdgesOnly = false;
	}

	/**
	 * The non-query end of any {@link Edge}s returned from a query must satisfy this predicate.
	 *
	 * See the package {@link gaffer.predicate.typevalue.impl} for possible predicates. It is possible
	 * to use your own predicates (i.e. predicates which are not part of Gaffer itself), but this requires the
	 * class to be installed on all of Accumulo's tablet servers' classpaths - see your administrator.
	 *
	 * @param typeValuePredicate
	 */
	@Override
	public void setOtherEndOfEdgePredicate(TypeValuePredicate typeValuePredicate) {
		try {
			if (!ClassLoadTestCache.checkLoad(connector, tableName, typeValuePredicate.getClass().getName(), TypeValuePredicate.class.getName())) {
				throw new IllegalArgumentException("This class is not on Accumulo's classpath (contact your system administrator).");
            }
		} catch (AccumuloSecurityException e) {
			throw new IllegalArgumentException("Exception setting other end of edge predicate: " + e);
		} catch (AccumuloException e) {
			throw new IllegalArgumentException("Exception setting other end of edge predicate: " + e);
		} catch (TableNotFoundException e) {
			throw new IllegalArgumentException("Exception setting other end of edge predicate: " + e);
		}
		this.otherEndOfEdgePredicate = typeValuePredicate;
	}

	/**
	 * The non-query end of any {@link Edge}s returned from a query must have a type in the provided
	 * {@link Set}.
	 *
	 * This overrides any previous calls to {@link #setOtherEndOfEdgePredicate}, {@link #setOtherEndOfEdgeHasType}
	 * or {@link #setOtherEndOfEdgeValueMatches}.
	 *
	 * @param allowedTypes
	 */
	@Override
	public void setOtherEndOfEdgeHasType(Set<String> allowedTypes) {
		this.otherEndOfEdgePredicate = new TypeInSetPredicate(allowedTypes);
	}

	/**
	 * The non-query end of any {@link Edge}s returned from a query must have a value that matches the
	 * provided regular expression.
	 *
	 * This overrides any previous calls to {@link #setOtherEndOfEdgePredicate}, {@link #setOtherEndOfEdgeHasType}
	 * or {@link #setOtherEndOfEdgeValueMatches}.
	 *
	 * @param pattern
	 */
	@Override
	public void setOtherEndOfEdgeValueMatches(Pattern pattern) {
		this.otherEndOfEdgePredicate = new ValueRegularExpressionPredicate(pattern);
	}

	/**
	 * This cancels any previous calls to {@link #setOtherEndOfEdgePredicate}, {@link #setOtherEndOfEdgeHasType}
	 * or {@link #setOtherEndOfEdgeValueMatches}.
	 */
	@Override
	public void removeOtherEndOfEdgePredicate() {
		this.otherEndOfEdgePredicate = null;
	}

	/**
	 * Setting this causes only {@link Statistic}s with the supplied names to be returned, e.g. if the set
	 * <code>statisticsToKeepByName</code> only contains "item_count", then {@link SetOfStatistics} returned
	 * will not contain any statistics with other names.
	 *
	 * Note that if methods are called which do not return {@link SetOfStatistics}, e.g. {@link #getGraphElements},
	 * then all statistics are automatically filtered out server-side to avoid the expense of sending them to
	 * the client.
	 *
	 * @param statisticsToKeepByName
	 */
	@Override
	public void setStatisticsToKeepByName(Set<String> statisticsToKeepByName) {
		this.statisticsToKeepByName = new HashSet<String>(statisticsToKeepByName);
		this.statisticsToRemoveByName = null;
	}

	/**
	 * Setting this causes {@link Statistic}s with the supplied names to not be returned, e.g. if the set
	 * <code>statisticsToRemoveByName</code> only contains "item_count", then {@link SetOfStatistics} returned
	 * will not contain statistics with this name.
	 *
	 * Note that if methods are called which do not return {@link SetOfStatistics}, e.g. {@link #getGraphElements},
	 * then all statistics are automatically filtered out server-side to avoid the expense of sending them to
	 * the client.
	 *
	 * @param statisticsToRemoveByName
	 */
	@Override
	public void setStatisticsToRemoveByName(Set<String> statisticsToRemoveByName) {
		this.statisticsToKeepByName = null;
		this.statisticsToRemoveByName = new HashSet<String>(statisticsToRemoveByName);
	}

	/**
	 * Allows the user to specify whether they want the results of queries to be rolled up
	 * over time and visibility, e.g. get an edge between A and B for all of the time
	 * period of the graph, rather than one edge per day (if graph elements were created
	 * with start and end dates differing by a day).
	 */
	@Override
	public void rollUpOverTimeAndVisibility(boolean rollUp) {
		this.rollUpOverTimeAndVisibility = rollUp;
	}
	
	/**
	 * Is rolling up over time and visibility currently set?
	 * 
	 * @return
	 */
	@Override
	public boolean isRollUpOverTimeAndVisibilityOn() {
		return this.rollUpOverTimeAndVisibility;
	}

	/**
	 * Sets a {@link Transform} to be applied to all results from a query. This is applied last, i.e. after all other
	 * filtering and roll-up.
	 *
	 * Note that the {@link Transform} is applied client-side and thus there is no requirement for the {@link Transform}
	 * to be installed on Accumulo's tablet servers' classpaths.
	 *
	 * @param transform
	 */
	@Override
	public void setPostRollUpTransform(Transform transform) {
		this.postRollUpTransform = transform;
	}

	/**
	 * This cancels out any previous calls to {@link #setPostRollUpTransform}.
	 */
	@Override
	public void removePostRollUpTransform() {
		this.postRollUpTransform = null;
	}

	/**
	 * For advanced users only: sets the maximum number of entries to include in a single
	 * {@link BatchScanner} when querying Accumulo. If not specified, a sensible
	 * default is used.
	 * 
	 * @param maxEntries
	 */
	public void setMaxEntriesForBatchScanner(int maxEntries) {
		if (maxEntries < 1) {
			throw new IllegalArgumentException("Must specify a maximum number of entries of at least one for batch scanner");
		}
		this.maxEntriesForBatchScanner = maxEntries;
	}
	
	/**
	 * For advanced users only: sets the maximum number of threads to be used by a
	 * {@link BatchScanner} when querying Accumulo. If not specified, a sensible
	 * default is used.
	 * 
	 * @param threads
	 */
	public void setMaxThreadsForBatchScanner(int threads) {
		if (threads < 1) {
			throw new IllegalArgumentException("Must specify at least one thread for batch scanner");
		}
		this.threadsForBatchScanner = threads;
	}

	/**
	 * For advanced users only: sets the maximum size of {@link BloomFilter} that can be passed to an
	 * iterator (this happens when method {@link #getGraphElementsWithStatisticsWithinSet(Iterable, boolean)}
	 * is called). If not specified, a sensible default is used.
	 *
	 * @param size
	 */
	public void setMaxBloomFilterToPassToAnIterator(int size) {
		if (size < 1) {
			throw new IllegalArgumentException("Must specify a size of at least 1 for the Bloom filter");
		}
		this.maxBloomFilterToPassToAnIterator = size;
	}

	/**
	 * For advanced users only: sets the desired false positive rate for the {@link BloomFilter}s that are
	 * passed to iterators (this happens when method {@link #getGraphElementsWithStatisticsWithinSet(Iterable, boolean)}
	 * is called). If not specified, a sensible default is used.
	 *
	 * @param falsePositiveRate
	 */
	public void setFalsePositiveRateForBloomFilter(double falsePositiveRate) {
		if (falsePositiveRate <= 0.0 || falsePositiveRate > 1.0) {
			throw new IllegalArgumentException("Must specify a false positive rate between 0.0 and 1.0 for the Bloom filter");
		}
		this.falsePositiveRateForBloomFilter = falsePositiveRate;
	}

	/**
	 * For advanced users only: sets the size of the client side {@link BloomFilter} that is used in the method
	 * {@link #getGraphElementsWithStatisticsWithinSet(Iterable, boolean)} when the second argument is <code>false</code>.
	 * If not specified, a sensible default is used.
	 *
	 * @param clientSideBloomFilterSize
	 */
	public void setClientSideBloomFilterSize(int clientSideBloomFilterSize) {
		if (clientSideBloomFilterSize < 1) {
			throw new IllegalArgumentException("Must specify a size of at least 1 for the Bloom filter");
		}
		this.clientSideBloomFilterSize = clientSideBloomFilterSize;
	}

	/**
	 * Adds all the provided {@link GraphElement} to the graph with an empty
	 * {@link SetOfStatistics}.
	 * 
	 * @param graphElements
	 */
	@Override
	public void addGraphElements(Iterable<GraphElement> graphElements) throws GraphAccessException {
		try {
			InsertIntoGraphUtilities.insertGraphElements(connector, tableName, bufferSizeForBatchWriter,
					timeOutForBatchWriter, numThreadsForBatchWriter, graphElements);
		} catch (MutationsRejectedException e) {
			throw new GraphAccessException("MutationsRejectedException in addGraphElements to AccumuloBackedGraph (connector =  "
					+ connector + ", table = " + tableName + ") " + e.getMessage());
		} catch (TableNotFoundException e) {
			throw new GraphAccessException("TableNotFoundException in addGraphElements to AccumuloBackedGraph (connector =  "
					+ connector + ", table = " + tableName + ") " + e.getMessage());
		}
	}

	/**
	 * Adds all the provided {@link GraphElementWithStatistics} to the graph.
	 * 
	 * @param graphElementsWithStatistics
	 */
	@Override
	public void addGraphElementsWithStatistics(Iterable<GraphElementWithStatistics> graphElementsWithStatistics) throws GraphAccessException {
		try {
			InsertIntoGraphUtilities.insertGraphElementsWithStatistics(connector, tableName, bufferSizeForBatchWriter,
					timeOutForBatchWriter, numThreadsForBatchWriter, graphElementsWithStatistics);
		} catch (MutationsRejectedException e) {
			throw new GraphAccessException("MutationsRejectedException in addGraphElementsWithStatistics to AccumuloBackedGraph (connector =  "
					+ connector + ", table = " + tableName + ") " + e.getMessage());
		} catch (TableNotFoundException e) {
			throw new GraphAccessException("TableNotFoundException in addGraphElementsWithStatistics to AccumuloBackedGraph (connector =  "
					+ connector + ", table = " + tableName + ") " + e.getMessage());
		}
	}

	/**
	 * Updates the provided {@link Configuration} with options that specify the view
	 * on the graph (i.e. the authorizations, the time window, the required summary types,
	 * whether we want to roll up over time and visibility, and whether we want entities
	 * only or edges only or entities and edges). This allows a MapReduce job that uses
	 * the {@link Configuration} to receive data that is subject to the same view as is
	 * on the graph.
	 *
	 * Note that this method will not produce the same {@link Edge} twice.
	 *
	 * @param conf
	 * @param accumuloConfig
	 */
	public void setConfiguration(Configuration conf, AccumuloConfig accumuloConfig) throws AccumuloSecurityException, IOException {
		addAccumuloInfoToConfiguration(conf, accumuloConfig);
		addRollUpAndAuthsToConfiguration(conf);
		Predicate<RawGraphElementWithStatistics> filterPredicate = createFilterPredicate();
		if (returnEntities && !returnEdges) {
			filterPredicate = andPredicates(filterPredicate, new RawGraphElementWithStatisticsPredicate(new IsEntityPredicate()));
		} else if (!returnEntities && returnEdges) {
			filterPredicate = andPredicates(filterPredicate, new RawGraphElementWithStatisticsPredicate(new IsEdgePredicate()));
		}
		filterPredicate = andPredicates(filterPredicate, new ReturnEdgesOnlyOncePredicate());
		addPreRollUpFilterToConfiguration(conf, filterPredicate);
		addPostRollUpTransformToConfiguration(conf);
	}

	/**
	 * Updates the provided {@link Configuration} with options that specify the view
	 * on the graph (i.e. the authorizations, the time window, the required summary types,
	 * whether we want to roll up over time and visibility, and whether we want entities
	 * only or edges only or entities and edges), and sets it to only return data that
	 * involves the given {@link TypeValue}. This allows a MapReduce job that uses
	 * the {@link Configuration} to receive data that only involves the given {@link
	 * TypeValue} and is subject to the same view as is on the graph.
	 *
	 * @param conf
	 * @param typeValue
	 * @param accumuloConfig
	 */
	public void setConfiguration(Configuration conf, TypeValue typeValue, AccumuloConfig accumuloConfig) throws AccumuloSecurityException, IOException {
		setConfiguration(conf, Collections.singleton(typeValue), accumuloConfig);
	}

	/**
	 * Updates the provided {@link Configuration} with options that specify the view
	 * on the graph (i.e. the authorizations, the time window, the required summary types,
	 * whether we want to roll up over time and visibility, and whether we want entities
	 * only or edges only or entities and edges), and sets it to only return data that
	 * involves the given {@link TypeValue}s. This allows a MapReduce job that uses
	 * the {@link Configuration} to receive data that only involves the given {@link
	 * TypeValue}s and is subject to the same view as is on the graph.
	 *
	 * Note that this method may cause the same {@link Edge} to be returned twice if
	 * both ends are in the provided set.
	 *
	 * @param conf
	 * @param typeValues
	 * @param accumuloConfig
	 */
	public void setConfiguration(Configuration conf, Collection<TypeValue> typeValues,
								 AccumuloConfig accumuloConfig) throws AccumuloSecurityException, IOException {
		// NB: Do not need to add the Entity or Edge only iterator here, as the ranges that are
		// created take care of that.
		addAccumuloInfoToConfiguration(conf, accumuloConfig);
		addRollUpAndAuthsToConfiguration(conf);
		Predicate<RawGraphElementWithStatistics> filterPredicate = andPredicates(createFilterPredicate(), createFilterPredicateForQuery());
		addPreRollUpFilterToConfiguration(conf, filterPredicate);
		Set<Range> ranges = new HashSet<Range>();
		for (TypeValue typeValue : typeValues) {
			ranges.add(ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue(), returnEntities, returnEdges));
		}
		InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges);
		addPostRollUpTransformToConfiguration(conf);
	}

	/**
	 * Updates the provided {@link Configuration} with options that specify the view
	 * on the graph (i.e. the authorizations, the time window, the required summary types,
	 * whether we want to roll up over time and visibility, and whether we want entities
	 * only or edges only or entities and edges), and sets it to only return data that
	 * is in the given {@link TypeValueRange}. This allows a MapReduce job that uses
	 * the {@link Configuration} to only receive data that involves the given {@link
	 * TypeValueRange} and is subject to the same view as is on the graph.
	 *
	 * Note that this method may cause the same {@link Edge} to be returned twice if
	 * both ends are in the provided range.
	 *
	 * @param conf
	 * @param typeValueRange
	 * @param accumuloConfig
	 */
	public void setConfigurationFromRanges(Configuration conf, TypeValueRange typeValueRange,
										   AccumuloConfig accumuloConfig) throws AccumuloSecurityException, IOException {
		setConfigurationFromRanges(conf, Collections.singleton(typeValueRange), accumuloConfig);
	}

	/**
	 * Updates the provided {@link Configuration} with options that specify the view
	 * on the graph (i.e. the authorizations, the time window, the required summary types,
	 * whether we want to roll up over time and visibility, and whether we want entities
	 * only or edges only or entities and edges), and sets it to only return data that
	 * is in the given {@link TypeValueRange}s. This allows a MapReduce job that uses
	 * the {@link Configuration} to only receive data that involves the given {@link
	 * TypeValueRange}s and is subject to the same view as is on the graph.
	 *
	 * Note that this method may cause the same {@link Edge} to be returned twice if
	 * both ends are in the provided ranges.
	 *
	 * @param conf
	 * @param typeValueRanges
	 * @param accumuloConfig
	 */
	public void setConfigurationFromRanges(Configuration conf, Collection<TypeValueRange> typeValueRanges,
										   AccumuloConfig accumuloConfig) throws AccumuloSecurityException, IOException {
		addAccumuloInfoToConfiguration(conf, accumuloConfig);
		addRollUpAndAuthsToConfiguration(conf);
		Predicate<RawGraphElementWithStatistics> filterPredicate = andPredicates(createFilterPredicate(), createFilterPredicateForQuery());
		if (returnEntities && !returnEdges) {
			filterPredicate = andPredicates(filterPredicate, new RawGraphElementWithStatisticsPredicate(new IsEntityPredicate()));
		} else if (!returnEntities && returnEdges) {
			filterPredicate = andPredicates(filterPredicate, new RawGraphElementWithStatisticsPredicate(new IsEdgePredicate()));
		}
		addPreRollUpFilterToConfiguration(conf, filterPredicate);
		Set<Range> ranges = new HashSet<Range>();
		for (TypeValueRange typeValueRange : typeValueRanges) {
			ranges.add(ConversionUtils.getRangeFromTypeValueRange(typeValueRange));
		}
		InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges);
		addPostRollUpTransformToConfiguration(conf);
	}

	/**
	 * Updates the provided {@link Configuration} with options that specify the view
	 * on the graph (i.e. the authorizations, the time window, the required summary types,
	 * whether we want to roll up over time and visibility), and sets it to only return edges
	 * that involve the given pair of {@link TypeValue}s. This allows a MapReduce job that uses
	 * the {@link Configuration} to only receive edges that involves the given pair of {@link
	 * TypeValue}s and is subject to the same view as is on the graph.
	 *
	 * @param conf
	 * @param typeValuePair
	 * @param accumuloConfig
	 */
	public void setConfigurationFromPairs(Configuration conf, Pair<TypeValue> typeValuePair,
										  AccumuloConfig accumuloConfig) throws AccumuloSecurityException, IOException {
		setConfigurationFromPairs(conf, Collections.singleton(typeValuePair), accumuloConfig);
	}

	/**
	 * Updates the provided {@link Configuration} with options that specify the view
	 * on the graph (i.e. the authorizations, the time window, the required summary types,
	 * whether we want to roll up over time and visibility), and sets it to only return edges
	 * that involve the given pairs of {@link TypeValue}s. This allows a MapReduce job that uses
	 * the {@link Configuration} to only receive edges that involves the given pairs of {@link
	 * TypeValue}s and is subject to the same view as is on the graph.
	 *
	 * @param conf
	 * @param typeValuePairs
	 * @param accumuloConfig
	 */
	public void setConfigurationFromPairs(Configuration conf, Collection<Pair<TypeValue>> typeValuePairs,
										  AccumuloConfig accumuloConfig) throws AccumuloSecurityException, IOException {
		// NB: Do not need to add the Entity or Edge only iterator here, as the ranges that are
		// created take care of that.
		addAccumuloInfoToConfiguration(conf, accumuloConfig);
		addRollUpAndAuthsToConfiguration(conf);
		Predicate<RawGraphElementWithStatistics> filterPredicate = andPredicates(createFilterPredicate(), createFilterPredicateForQuery());
		addPreRollUpFilterToConfiguration(conf, filterPredicate);
		Set<Range> ranges = new HashSet<Range>();
		for (Pair<TypeValue> typeValuePair : typeValuePairs) {
			ranges.add(ConversionUtils.getRangeFromPairOfTypesAndValues(typeValuePair.getFirst().getType(),
					typeValuePair.getFirst().getValue(), typeValuePair.getSecond().getType(),
					typeValuePair.getSecond().getValue()));
		}
		InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges);
		addPostRollUpTransformToConfiguration(conf);
	}

	/**
	 * Sets the post roll-up transform on the {@link Configuration}. This is applied in the {@link RecordReader}
	 * created by {@link ElementInputFormat}.
	 *
	 * @param conf
	 */
	protected void addPostRollUpTransformToConfiguration(Configuration conf) throws IOException {
		Transform transform = getPostRollUpTransform();
		if (transform != null) {
			conf.set(ElementInputFormat.POST_ROLL_UP_TRANSFORM, WritableToStringConverter.serialiseToString(transform));
		}
	}

	/**
	 * Sets the roll-up, authorizations, summary types, and time window on the {@link Configuration}. NB: It
	 * deliberately does not set the entity or edge only iterator as whether that is needed or not
	 * depends on what ranges are set (if you ask for Entity only information for a set of {@link
	 * TypeValue}s for example, the ranges that are created automatically only return entities).
	 *
	 * @param conf
	 */
	protected void addRollUpAndAuthsToConfiguration(Configuration conf) {
		// Roll-up
		if (rollUpOverTimeAndVisibility) {
			InputConfigurator.addIterator(AccumuloInputFormat.class, conf, TableUtils.getRollUpOverTimeAndVisibilityIteratorSetting());
		}
		// Auths
		InputConfigurator.setScanAuthorizations(AccumuloInputFormat.class, conf, authorizations);
	}

	/**
	 * Configures the pre roll-up filtering iterator to run with the provided {@link Predicate<RawGraphElementWithStatistics>}.
	 *
	 * @param conf
	 * @param predicate
	 */
	protected void addPreRollUpFilterToConfiguration(Configuration conf, Predicate<RawGraphElementWithStatistics> predicate) {
		if (predicate != null) {
			InputConfigurator.addIterator(AccumuloInputFormat.class, conf, TableUtils.getPreRollUpFilterIteratorSetting(predicate));
		}
	}

	/**
	 * Sets the Accumulo information on the provided {@link Configuration}, i.e. the table name,
	 * the connector, the authorizations, and the Zookeepers. Note that if no zookeepers are provided
	 * then it is assumed that a {@link MockInstance} is being used.
	 *
	 * @param conf
	 * @param accumuloConfig
	 * @throws AccumuloSecurityException
	 */
	protected void addAccumuloInfoToConfiguration(Configuration conf, AccumuloConfig accumuloConfig)
			throws AccumuloSecurityException {
		// Table
		InputConfigurator.setInputTableName(AccumuloInputFormat.class, conf, tableName);
		// Connector
		InputConfigurator.setConnectorInfo(AccumuloInputFormat.class, conf, accumuloConfig.getUserName(),
				new PasswordToken(accumuloConfig.getPassword()));
		// Authorizations
		InputConfigurator.setScanAuthorizations(AccumuloInputFormat.class, conf, authorizations);
		// Zookeeper
		if (accumuloConfig.getZookeepers() != null && !accumuloConfig.getZookeepers().equals(AccumuloConfig.MOCK_ZOOKEEPERS)) {
			InputConfigurator.setZooKeeperInstance(AccumuloInputFormat.class, conf, new ClientConfiguration()
					.withInstance(accumuloConfig.getInstanceName()).withZkHosts(accumuloConfig.getZookeepers()));
		} else {
			InputConfigurator.setMockInstance(AccumuloInputFormat.class, conf, accumuloConfig.getInstanceName());
		}
	}

	/**
	 * For advanced users only: sets the size in bytes of the maximum memory to batch
	 * before writing data to Accumulo using {@link #addGraphElements} or
	 * {@link #addGraphElementsWithStatistics}.
	 * 
	 * @param bufferSize
	 */
	public void setBufferSizeForInserts(long bufferSize) {
		if (bufferSize < 1L) {
			throw new IllegalArgumentException("Must specify a strictly positive buffer size for inserts");
		}
		this.bufferSizeForBatchWriter = bufferSize;
	}
	
	/**
	 * For advanced users only: the maximum time in milliseconds to hold a batch before
	 * writing data to Accumulo using {@link #addGraphElements} or
	 * {@link #addGraphElementsWithStatistics}.
	 * 
	 * @param timeOut
	 */
	public void setTimeOutForInserts(long timeOut) {
		if (timeOut < 1L) {
			throw new IllegalArgumentException("Must specify a strictly positive timeout for inserts");
		}
		this.timeOutForBatchWriter = timeOut;
	}
	
	/**
	 * For advanced users only: the maximum number of threads to use for writing data to
	 * the tablet servers in Accumulo using {@link #addGraphElements} or
	 * {@link #addGraphElementsWithStatistics}.
	 * 
	 * @param numThreads
	 */
	public void setNumThreadsForInserts(int numThreads) {
		if (numThreads < 1) {
			throw new IllegalArgumentException("Must specify at least one thread for inserts");
		}
		this.numThreadsForBatchWriter = numThreads;
	}

	@Override
	public String toString() {
		return toString(false);
	}

	public String toString(boolean fullDetails) {
		if (fullDetails) {
			return "AccumuloBackedGraph{" +
					"connector=" + connector +
					", tableName='" + tableName + '\'' +
					", authorizations=" + authorizations +
					", maxEntriesForBatchScanner=" + maxEntriesForBatchScanner +
					", threadsForBatchScanner=" + threadsForBatchScanner +
					", bufferSizeForBatchWriter=" + bufferSizeForBatchWriter +
					", timeOutForBatchWriter=" + timeOutForBatchWriter +
					", numThreadsForBatchWriter=" + numThreadsForBatchWriter +
					", timePredicate=" + timePredicate +
					", rollUpOverTimeAndVisibility=" + rollUpOverTimeAndVisibility +
					", returnEntities=" + returnEntities +
					", returnEdges=" + returnEdges +
					", summaryTypePredicate=" + summaryTypePredicate +
					'}';
		}
		return "AccumuloBackedGraph { User = " + connector.whoami() + ", Table = " + tableName + " }";
	}

	/**
	 * Returns a new AccumuloBackedGraph that is a clone of this one. This includes cloning the view
	 * that is currently set on the graph. Note that it does clone the {@link Connector} to Accumulo.
	 *
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public AccumuloBackedGraph cloneGraph() throws AccumuloException, AccumuloSecurityException {
		AccumuloBackedGraph clone = new AccumuloBackedGraph(connector, tableName);
		addParams(clone);
		return clone;
	}

	protected void addParams(AccumuloBackedGraph other) {
		// Authorizations
		other.authorizations = new Authorizations(authorizations.getAuthorizations());
		// Parameters for query
		other.maxEntriesForBatchScanner = maxEntriesForBatchScanner;
		other.threadsForBatchScanner = threadsForBatchScanner;
		// Parameters for inserts
		other.bufferSizeForBatchWriter = bufferSizeForBatchWriter;
		other.timeOutForBatchWriter = timeOutForBatchWriter;
		other.numThreadsForBatchWriter = numThreadsForBatchWriter;
		// Parameters for Bloom filters
		other.maxBloomFilterToPassToAnIterator = maxBloomFilterToPassToAnIterator;
		other.falsePositiveRateForBloomFilter = falsePositiveRateForBloomFilter;
		other.clientSideBloomFilterSize = clientSideBloomFilterSize;
		// View on the graph
		other.timePredicate = timePredicate;
		other.rollUpOverTimeAndVisibility = rollUpOverTimeAndVisibility;
		other.returnEntities = returnEntities;
		other.returnEdges = returnEdges;
		other.summaryTypePredicate = summaryTypePredicate;
		other.undirectedEdgesOnly = undirectedEdgesOnly;
		other.directedEdgesOnly = directedEdgesOnly;
		other.outgoingEdgesOnly = outgoingEdgesOnly;
		other.incomingEdgesOnly = incomingEdgesOnly;
		other.otherEndOfEdgePredicate = otherEndOfEdgePredicate;
		other.postRollUpTransform = postRollUpTransform;
		other.statisticsToKeepByName = statisticsToKeepByName;
		other.statisticsToRemoveByName = statisticsToRemoveByName;
	}

	/**
	 * Creates a {@link Predicate} of {@link RawGraphElementWithStatistics} using the time predicate, the summary type
	 * predicate, and predicates based on whether only directed or only undirected edges are wanted. This
	 * predicate should be applied to all queries and jobs over the whole table.
	 *
	 * @return
	 */
	protected Predicate<RawGraphElementWithStatistics> createFilterPredicate() {
		Predicate<RawGraphElementWithStatistics> predicate = null;
		if (timePredicate != null) {
			predicate = andPredicates(predicate, new RawGraphElementWithStatisticsPredicate(timePredicate));
		}
		if (summaryTypePredicate != null) {
			predicate = andPredicates(predicate, new RawGraphElementWithStatisticsPredicate(summaryTypePredicate));
		}
		if (otherEndOfEdgePredicate != null) {
			predicate = andPredicates(predicate, new OtherEndOfEdgePredicate(otherEndOfEdgePredicate));
		}
		if (undirectedEdgesOnly) {
			predicate = andPredicates(predicate, new RawGraphElementWithStatisticsPredicate(new UndirectedEdgePredicate()));
		} else if (directedEdgesOnly) {
			predicate = andPredicates(predicate, new RawGraphElementWithStatisticsPredicate(new DirectedEdgePredicate()));
		}
		return predicate;
	}

	/**
	 * Creates a {@link Predicate} of {@link RawGraphElementWithStatistics} based on whether only outgoing edges or only
	 * incoming edges are wanted. This is only meaningful for queries, i.e. not for jobs which run over the
	 * whole table.
	 *
	 * NB: If adding predicates into this method, check whether the methods {@link #getGraphElementsWithStatisticsWithinSet(Iterable, boolean)}
	 * and {@link #getGraphElementsWithStatisticsBetweenSets(Iterable, Iterable, boolean)} need to use those predicates,
	 * as those methods currently do not use this method as they should ignore the outgoing edges only and incoming edges
	 * only options.
	 *
	 * @return
	 */
	protected Predicate<RawGraphElementWithStatistics> createFilterPredicateForQuery() {
		Predicate<RawGraphElementWithStatistics> predicate = null;
		if (outgoingEdgesOnly) {
			predicate = andPredicates(predicate, new OutgoingEdgePredicate());
		} else if (incomingEdgesOnly) {
			predicate = andPredicates(predicate, new IncomingEdgePredicate());
		}
		return predicate;
	}

	protected StatisticsTransform calculateStatisticsTransform() {
		if (statisticsToKeepByName != null) {
			return new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.KEEP, statisticsToKeepByName);
		}
		if (statisticsToRemoveByName != null) {
			return new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.REMOVE, statisticsToRemoveByName);
		}
		return null;
	}

	public static Predicate<RawGraphElementWithStatistics> andPredicates(Predicate<RawGraphElementWithStatistics> predicate1,
																   Predicate<RawGraphElementWithStatistics> predicate2) {
		if (predicate1 == null) {
			return predicate2;
		}
		if (predicate2 == null) {
			return predicate1;
		}
		return new CombinedPredicates<RawGraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.AND);
	}

	/**
	 * Putting this into a method allows subclasses to override it so that they can add their own specific post
	 * rollup transforms to the ones specified by the user.
	 *
	 * @return
	 */
	protected Transform getPostRollUpTransform() {
		return postRollUpTransform;
	}

}
