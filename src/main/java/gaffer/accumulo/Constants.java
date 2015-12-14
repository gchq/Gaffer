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

/**
 * Contains a variety of constants needed for Accumulo.
 */
public class Constants {

	private Constants() {}
	
	// Delimiter for splitting fields within rowKey, columnFamily, etc
	public final static char DELIMITER = '\u0000';
	
	// Used to specify the end of a range - for example all Edges involving a
	// type-value can be found in a range that starts with the type-value and ends
	// with the type-value plus DELIMITER_PLUS_ONE.
	public final static char DELIMITER_PLUS_ONE = '\u0001';

	// Batch scanner default parameters - these are used by default in AccumuloBackedGraph, but can be over-ridden.
	public final static int MAX_ENTRIES_FOR_BATCH_SCANNER = 50000;
	public final static int THREADS_FOR_BATCH_SCANNER = 10;
	
	// Batch writer default parameters - these are used by default in AccumuloBackedGraph, but can be over-ridden.
	public final static long MAX_BUFFER_SIZE_FOR_BATCH_WRITER = 1000000L; // This is measured in bytes
	public final static long MAX_TIME_OUT_FOR_BATCH_WRITER = 1000L; // This is measured in milliseconds
	public final static int NUM_THREADS_FOR_BATCH_WRITER = 10;

	// Bloom filter parameters
	// Bloom filters are used in iterators by certain methods to do filtering, and also
	// to do client side secondary filtering.
	// 	- Maximum size of a Bloom filter that should be passed to an iterator in bits.
	public final static int MAX_SIZE_BLOOM_FILTER = 1024 * 1024 * 8;
	// 	- Desired false positive rate for Bloom filter that is passed to an iterator.
	public final static double FALSE_POSITIVE_RATE_FOR_BLOOM_FILTER = 0.0001;
	//	- Size of the client side Bloom filter used for secondary testing in bits.
	public final static int SIZE_CLIENT_SIDE_BLOOM_FILTER = 100 * 1024 * 1024 * 8;

	// Iterator priorities (remember that lower number ones run first).
	// A brief summary of the role of these iterators:
	//		- The age off iterator is applied first. This removes date that is too old. The
	//			decision as to whether it is too old or not depends on the timestamp of the
	//			key. The timestamp of the key is always the same as the end date of the edge.
	//			(Actually after iterators further up the stack are applied the timestamp of
	//			the edge might not be that, but the timestamp shouldn't be used.)
	//			This is done at all 3 scopes, so it is persistent (i.e. we delete old data).
	//		- The set of statistics combiner is applied next. This merges together
	//			the values (which are serialised SetOfStatistics) for identical row keys.
	//			This is merging in data from the same edge and the same time period and the
	//			same visibility.
	// 			This is done at all 3 scopes, so it is persistent (i.e. the merge can't be
	//			undone).
	// 		- The pre rollup filtering iterator (RawElementWithStatsFilter) is applied next.
	// 			This is configured using a Predicate of RawGraphElementWithStatistics. This
	//			does things like: remove summary types that are not of interest; ensure that
	//			only data from the correct time window is returned, etc.
	//			This is applied at scan time only.
	// 		- The statistics transform iterator is applied next. This applies the provided
	//			statistic transform, typically to remove unwanted statistics. This is applied
	// 			at scan time only.
	//		- The roll up over time and visibility combiner is applied next. This produces
	//			the final entity or edge that the user will see. Within a (source, destination, direction,
	// 			edge type and subtype) group it merges together data. So it joins together
	//			information from different time windows and different visibilities into one
	//			entity or edge. The corresponding values are also merged. This allows the user to see a
	//			summarised view of everything about the particular type of relationship between
	//			a given pair of type-values, or of everything about a particular type of
	//			property of a type-value.
	//			This is applied at scan time only.
	public final static int AGE_OFF_ITERATOR_PRIORITY = 10; // Applied during major compactions, minor compactions and scans.
	public final static int SET_OF_STATISTICS_COMBINER_PRIORITY = 20; // Applied during major compactions, minor compactions and scans.
	public final static int PRE_ROLLUP_FILTER_PRIORITY = 30; // Applied only during scans.
	public final static int STATISTICS_TRANSFORM_PRIORITY = 40; // Applied only during scans.
	public final static int ROLL_UP_OVER_TIME_AND_VISIBILITY_COMBINER_PRIORITY = 50; // Applied only during scans.

	// RawGraphElementWithStatistics Filtering iterator
	public final static String RAW_ELEMENT_WITH_STATS_PREDICATE_FILTER_NAME = "RawElementWithStatsPredicateFilter";
	public final static String RAW_ELEMENT_WITH_STATS_PREDICATE = "RawElementWithStatsPredicate";

	// StatisticTransform iterator
	public final static String STATISTIC_TRANSFORM_ITERATOR_NAME = "StatisticTransform";
	public final static String STATISTIC_TRANSFORM = "SerialisedStatisticTransform";

	// Errors in iterators
	public final static String ERROR_IN_SET_OF_STATISTICS_COMBINER_ITERATOR = "DeserialisationErrorInSetOfStatisticsCombiner";
	public final static String ERROR_IN_ROLL_UP_OVER_TIME_AND_VISIBILITY_ITERATOR = "DeserialisationErrorInRollUpOverTimeAndVisibility";

}
