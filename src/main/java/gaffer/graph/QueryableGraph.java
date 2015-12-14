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
package gaffer.graph;

import gaffer.Pair;
import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.EdgeWithStatistics;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.summarytype.SummaryTypePredicate;

import java.util.Date;
import java.util.Set;
import java.util.regex.Pattern;

import gaffer.predicate.time.TimePredicate;
import gaffer.predicate.typevalue.TypeValuePredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.Statistic;

/**
 * Interface that any graph that can be queried for {@link GraphElementWithStatistics}
 * must satisfy.
 * 
 * There are two types of method in the interface. The first set a view on the graph, i.e.
 * specify what time period is of interest, what security labels are allowed, whether we
 * want both {@link Entity}s and {@link Edge}s or just {@link Entity}s or just {@link Edge}s,
 * what summary types of {@link Entity}s and {@link Edge}s we want and whether we want the data
 * rolled up over time and visibility.
 * 
 * The second type of method allows users to get data from the graph, given an
 * {@link Iterable} of {@link TypeValue}s of interest.
 */
public interface QueryableGraph {

	// Methods to query the graph. These return data from within the current view of the
	// graph, i.e. from the time-window of interest, with the required authorizations and
	// with the summary types required.
	
	/**
	 * Returns all {@link GraphElement}s involving the given {@link TypeValue},
	 * subject to the view that's currently defined on the graph (i.e. the authorizations, the
	 * time window, the required summary types, whether we want entities and edges or just entities or
	 * just edges, and whether we want to roll up over time and visibility).
	 */
	Iterable<GraphElement> getGraphElements(TypeValue typeValue);
	
	/**
	 * Returns all {@link GraphElement}s involving the given {@link TypeValue}s from
	 * the provided {@link Iterable}, subject to the view that's currently defined on the graph
	 * (i.e. the authorizations, the time window, the required summary types, whether we want entities and
	 * edges or just entities or just edges, and whether we want to roll up over time and visibility).
	 */
	Iterable<GraphElement> getGraphElements(Iterable<TypeValue> typeValues);
	
	/**
	 * Returns all {@link GraphElementWithStatistics}s involving the given {@link TypeValue},
	 * subject to the view that's currently defined on the graph (i.e. the authorizations, the
	 * time window, the required summary types, whether we want entities and edges or just entities or
	 * just edges, and whether we want to roll up over time and visibility).
	 */
	Iterable<GraphElementWithStatistics> getGraphElementsWithStatistics(TypeValue typeValue);

	/**
	 * Returns all {@link GraphElementWithStatistics}s involving the given {@link TypeValue}s from
	 * the provided {@link Iterable}, subject to the view that's currently defined on the graph
	 * (i.e. the authorizations, the time window, the required summary types, whether we want entities and
	 * edges or just entities or just edges, and whether we want to roll up over time and visibility).
	 */
	Iterable<GraphElementWithStatistics> getGraphElementsWithStatistics(Iterable<TypeValue> typeValues);

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
	 */
	Iterable<EdgeWithStatistics> getEdgesWithStatisticsFromPair(Pair<TypeValue> typeValuePair);

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
	 */
	Iterable<EdgeWithStatistics> getEdgesWithStatisticsFromPairs(Iterable<Pair<TypeValue>> typeValuePairs);
	
	/**
	 * Returns all {@link GraphElementWithStatistics} from within the given {@link TypeValueRange}s,
	 * i.e. for which the type-value (in the case of an {@link Entity}) or one of the two type-values
	 * (in the case of an {@link Edge}) is within the range. This is subject to the view that's currently
	 * defined on the graph (i.e. the authorizations, the time window, the required summary types, whether we
	 * want entities and edges or just entities or just edges, and whether we want to roll up over time and
	 * visibility).
	 */
	Iterable<GraphElementWithStatistics> getGraphElementsWithStatisticsFromRanges(Iterable<TypeValueRange> typeValueRanges);
	
	// Methods to create a view on the graph. A view is determined by the time window,
	// the security labels that the user has permission to see, the summary types that the
	// user wants to see and whether they want to roll graph elements up over time
	// and visibility.
	
	/**
	 * Set the time window of interest. For a {@link GraphElement} to be returned from the graph,
	 * its time window must be a subset of the time period of interest specified by calling this
	 * method, i.e. the start date of the element must be the same as or after the start of the time
	 * period of interest and the end date of the element must be the same as or before the end of
	 * the time period of interest.
	 */
	void setTimeWindow(Date startDate, Date endDate);

	/**
	 * Set the time window of interest to be everything after the given {@link Date}. For a
	 * {@link GraphElement} to be returned from the graph, the start of its time window must
	 * be the same as the given {@link Date} or after it.
	 */
	void setTimeWindowToEverythingAfter(Date startTimeWindow);

	/**
	 * Set the time window of interest to be everything before the given {@link Date}. For a
	 * {@link GraphElement} to be returned from the graph, the end of its time window must
	 * be the same as the given {@link Date} or before it.
	 */
	void setTimeWindowToEverythingBefore(Date endTimeWindow);

	/**
	 * Sets a predicate to determine whether {@link GraphElement}s are wanted based on their
	 * start and end dates. This is applied before roll-up.
	 *
	 * Calling this method cancels any previous calls to either {@link #setTimeWindow(Date, Date)},
	 * {@link #setTimeWindowToEverythingAfter(Date)} or {@link #setTimeWindowToEverythingBefore(Date)}.
	 *
	 * @param timePredicate
	 */
	void setTimePredicate(TimePredicate timePredicate);

	/**
	 * Set the time window of interest to be all time.
	 */
	void setTimeWindowToAllTime();

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
	 * @param summaryTypePredicate
	 */
	void setSummaryTypePredicate(SummaryTypePredicate summaryTypePredicate);
	
	/**
	 * Sets the summary types of data to return from queries - for an {@link Edge} to be returned
	 * the edge summary type must be one of the types specified by this method, for an {@link Entity}
	 * to be returned its summary type must be one of the types specified by this method.
	 * 
	 * If {@link #setSummaryTypesAndSubTypes} is called after this method is called then it will
	 * override the effect of this method, i.e. the user must choose whether to specify the
	 * types they want by summary type only (i.e. I want summary type X whatever the subtype) or
	 * by both summary type and subtype (I want summary type X with subtype Y only). 
	 */
	void setSummaryTypes(Set<String> summaryTypes);

	/**
	 * Sets the summary types of data to return from queries - for an {@link Edge} to be returned
	 * the edge summary type must be one of the types specified by this method, for an {@link Entity}
	 * to be returned its summary type must be one of the types specified by this method.
	 *
	 * If {@link #setSummaryTypesAndSubTypes} is called after this method is called then it will
	 * override the effect of this method, i.e. the user must choose whether to specify the
	 * types they want by summary type only (i.e. I want summary type X whatever the subtype) or
	 * by both summary type and subtype (I want summary type X with subtype Y only).
	 */
	void setSummaryTypes(String... summaryTypes);
	
	/**
	 * Sets the summary types of data to return from queries - for an {@link Edge} to be returned
	 * the edge summary type and subtype must be as specified by this method, for an {@link Entity}
	 * to be returned its summary type and subtype must be one of the type/subtype pairs
	 * specified by this method.
	 * 
	 * If {@link #setSummaryTypes} is called after this method is called then it will
	 * override the effect of this method, i.e. the user must choose whether to specify the
	 * types they want by summary type only (i.e. I want summary type X whatever the subtype) or by
	 * both summary type and subtype (I want summary type X with subtype Y only). 
	 */
	void setSummaryTypesAndSubTypes(Set<Pair<String>> summaryTypesAndSubTypes);

	/**
	 * Sets the summary types of data to return from queries - for an {@link Edge} to be returned
	 * the edge summary type and subtype must be as specified by this method, for an {@link Entity}
	 * to be returned its summary type and subtype must be one of the type/subtype pairs
	 * specified by this method.
	 *
	 * If {@link #setSummaryTypes} is called after this method is called then it will
	 * override the effect of this method, i.e. the user must choose whether to specify the
	 * types they want by summary type only (i.e. I want summary type X whatever the subtype) or by
	 * both summary type and subtype (I want summary type X with subtype Y only).
	 */
	void setSummaryTypesAndSubTypes(Pair<String>... summaryTypesAndSubTypes);
	
	/**
	 * Sets the summary types of data to return from queries to be all types, i.e. {@link Entity}s
	 * and {@link Edge}s will be returned whatever their summary type and subtype. Calling this
	 * method overrides previous calls to {@link #setSummaryTypes} or {@link #setSummaryTypesAndSubTypes}.
	 */
	void setReturnAllSummaryTypesAndSubTypes();
	
	/**
	 * Allows the user to specify that they only want {@link Entity}s to be returned by queries.
	 */
	void setReturnEntitiesOnly();
	
	/**
	 * Allows the user to specify that they only want {@link Edge}s to be returned by queries.
	 */
	void setReturnEdgesOnly();
	
	/**
	 * Allows the user to specify that they want both {@link Entity}s and {@link Edge}s
	 * to be returned by queries.
	 */
	void setReturnEntitiesAndEdges();

	/**
	 * Allows the user to specify that if there are any {@link Edge}s in their results
	 * then they only want to receive undirected ones. Note that setting this option does
	 * not cause all {@link Entity}s to be excluded from the results - if that is desired
	 * then {@link #setReturnEdgesOnly} should be called as well as calling this method.
	 *
	 * This overrides any previous calls to {@link #setDirectedEdgesOnly}.
	 */
	void setUndirectedEdgesOnly();

	/**
	 * Allows the user to specify that if there are any {@link Edge}s in their results
	 * then they only want to receive directed ones. Note that setting this option does
	 * not cause all {@link Entity}s to be excluded from the results - if that is desired
	 * then {@link #setReturnEdgesOnly} should be called as well as calling this method.
	 *
	 * This overrides any previous calls to {@link #setUndirectedEdgesOnly}.
	 */
	void setDirectedEdgesOnly();

	/**
	 * This cancels out any previous calls to either {@link #setDirectedEdgesOnly()} or
	 * {@link #setUndirectedEdgesOnly()}.
	 */
	void setUndirectedAndDirectedEdges();

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
	void setOutgoingEdgesOnly();

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
	void setIncomingEdgesOnly();

	/**
	 * This cancels out any previous calls to either {@link #setOutgoingEdgesOnly} or {@link #setIncomingEdgesOnly}.
	 */
	void setOutgoingAndIncomingEdges();

	/**
	 * The non-query end of any {@link Edge}s returned from a query must satisfy this predicate.
	 */
	void setOtherEndOfEdgePredicate(TypeValuePredicate typeValuePredicate);

	/**
	 * The non-query end of any {@link Edge}s returned from a query must have a type in the provided
	 * {@link Set}.
	 *
	 * This overrides any previous calls to {@link #setOtherEndOfEdgePredicate}, {@link #setOtherEndOfEdgeHasType}
	 * or {@link #setOtherEndOfEdgeValueMatches}.
	 *
	 * @param allowedTypes
	 */
	void setOtherEndOfEdgeHasType(Set<String> allowedTypes);

	/**
	 * The non-query end of any {@link Edge}s returned from a query must have a value that matches the
	 * provided regular expression.
	 *
	 * This overrides any previous calls to {@link #setOtherEndOfEdgePredicate}, {@link #setOtherEndOfEdgeHasType}
	 * or {@link #setOtherEndOfEdgeValueMatches}.
	 *
	 * @param pattern
	 */
	void setOtherEndOfEdgeValueMatches(Pattern pattern);

	/**
	 * This cancels any previous calls to {@link #setOtherEndOfEdgePredicate}, {@link #setOtherEndOfEdgeHasType}
	 * or {@link #setOtherEndOfEdgeValueMatches}.
	 */
	void removeOtherEndOfEdgePredicate();

	/**
	 * Setting this causes only {@link Statistic}s with the supplied names to be returned, e.g. if the set
	 * <code>statisticsToKeepByName</code> only contains "item_count", then {@link SetOfStatistics} returned
	 * will not contain any statistics with other names.
	 *
	 * @param statisticsToKeepByName
	 */
	void setStatisticsToKeepByName(Set<String> statisticsToKeepByName);

	/**
	 * Setting this causes {@link Statistic}s with the supplied names to not be returned, e.g. if the set
	 * <code>statisticsToRemoveByName</code> only contains "item_count", then {@link SetOfStatistics} returned
	 * will not contain statistics with this name.
	 *
	 * @param statisticsToRemoveByName
	 */
	void setStatisticsToRemoveByName(Set<String> statisticsToRemoveByName);

	/**
	 * Allows the user to specify whether they want the results of queries to be rolled up
	 * over time and visibility, e.g. get an edge between A and B for all of the time
	 * period of the graph, rather than one edge per day (if graph elements were created
	 * with start and end dates differing by a day).
	 */
	void rollUpOverTimeAndVisibility(boolean roll);

	/**
	 * Is rolling up over time and visibility currently set?
	 *
	 * @return
	 */
	boolean isRollUpOverTimeAndVisibilityOn();

	/**
	 * Sets a {@link Transform} to be applied to all results from a query. This is applied last, i.e. after all other
	 * filtering and roll-up.
	 *
	 * @param transform
	 */
	void setPostRollUpTransform(Transform transform);

	/**
	 * This cancels out any previous calls to {@link #setPostRollUpTransform}.
	 */
	void removePostRollUpTransform();
}
