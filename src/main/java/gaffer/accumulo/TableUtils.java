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

import gaffer.accumulo.bloomfilter.ElementFunctor;
import gaffer.accumulo.iterators.*;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.graph.wrappers.GraphElement;
import gaffer.predicate.Predicate;
import gaffer.statistics.SetOfStatistics;

import java.io.IOException;
import java.util.EnumSet;

import gaffer.statistics.transform.StatisticsTransform;
import gaffer.utils.WritableToStringConverter;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to create a table for Gaffer data, and to add iterators to scanners. The method
 * that creates a table ensures that the correct iterators are set and that the correct Bloom filter
 * is enabled.
 */
public class TableUtils {

	private final static Logger LOGGER = LoggerFactory.getLogger(TableUtils.class);

	private TableUtils() {}

	/**
	 * Ensures that the table exists, otherwise it creates it and sets it up to receive Gaffer data.
	 * 
	 * @param connector  A connector to an Accumulo instance
	 * @param tableName  The name of the table
	 * @param ageOffTimeInMilliseconds  The age-off time for data in milliseconds
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public static void ensureTableExists(Connector connector, String tableName, long ageOffTimeInMilliseconds)
			throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		if (!connector.tableOperations().exists(tableName)) {
			try {
				TableUtils.createTable(connector, tableName, ageOffTimeInMilliseconds);
			} catch (TableExistsException e) {
				// Someone else got there first, never mind...
			}
		}
	}

	/**
	 * Creates a table for Gaffer data and: enables the correct Bloom filter; removes the versioning
	 * iterator; adds the {@link SetOfStatisticsCombiner}; and sets the {@link AgeOffFilter}
	 * to the specified time period.
	 * 
	 * @param connector  A connector to an Accumulo instance
	 * @param tableName  The name of the table
	 * @param ageOffTimeInMilliseconds  The age-off time for data in milliseconds
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 * @throws TableNotFoundException
	 */
	public static void createTable(Connector connector, String tableName, long ageOffTimeInMilliseconds)
			throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		// Create table
		connector.tableOperations().create(tableName);

		// Enable Bloom filters using ElementFunctor
		LOGGER.info("Enabling Bloom filter on table");
		connector.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ENABLED.getKey(), "true");
		connector.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), ElementFunctor.class.getName());
		LOGGER.info("Bloom filter enabled");

		// Remove versioning iterator from table for all scopes
		LOGGER.info("Removing versioning iterator");
		EnumSet<IteratorScope> iteratorScopes = EnumSet.allOf(IteratorScope.class);
		connector.tableOperations().removeIterator(tableName, "vers", iteratorScopes);
		LOGGER.info("Versioning iterator removed");

		// Add SetOfStatisticsCombiner iterator to table for all scopes
		LOGGER.info("Adding SetOfStatisticsCombiner iterator to table for all scopes");
		connector.tableOperations().attachIterator(tableName, TableUtils.getSetOfStatisticsCombinerIteratorSetting());
		LOGGER.info("Added SetOfStatisticsCombiner iterator to table for all scopes");

		// Add age off iterator to table for all scopes
		LOGGER.info("Adding age off iterator to table for all scopes");
		connector.tableOperations().attachIterator(tableName, getAgeOffIteratorSetting(ageOffTimeInMilliseconds));
		LOGGER.info("Added age off iterator to table for all scopes");
	}

	/**
	 * Adds an iterator to a {@link Scanner} that filters out rows according to the provided
	 * predicate.
	 *
	 * @param scanner  A scanner
	 * @param predicate  A predicate to apply as part of the scanner
	 */
	public static void addPreRollUpFilterIteratorToScanner(ScannerBase scanner, Predicate<RawGraphElementWithStatistics> predicate) {
		scanner.addScanIterator(getPreRollUpFilterIteratorSetting(predicate));
	}

	/**
	 * Adds an iterator to a {@link Scanner} that transforms the {@link SetOfStatistics} according
	 * to the provided {@link StatisticsTransform}.
	 *
	 * @param scanner  A scanner
	 * @param transform  The StatisticsTransform to be applied
	 */
	public static void addStatisticsTransformToScanner(ScannerBase scanner, StatisticsTransform transform) {
		scanner.addScanIterator(getStatisticTransformIteratorSetting(transform));
	}

	/**
	 * Adds an iterator to a {@link Scanner} that merges {@link GraphElement}s which only differ
	 * in the time period and visibility into one element. The associated {@link SetOfStatistics}
	 * are automatically merged. The visibilities are 'and'ed together.
	 *
	 * @param scanner  A scanner
	 */
	public static void addRollUpOverTimeAndVisibilityIteratorToScanner(ScannerBase scanner) {
		scanner.addScanIterator(getRollUpOverTimeAndVisibilityIteratorSetting());
	}

	/**
	 * Returns an {@link IteratorSetting} that specifies the age off iterator.
	 *
	 * @param ageOffTimeInMilliseconds  The age-off time for data in milliseconds
	 * @return An IteratorSetting applying the age-off iterator with the given age-off time
	 */
	public static IteratorSetting getAgeOffIteratorSetting(long ageOffTimeInMilliseconds) {
		IteratorSetting ageOffIteratorSetting = new IteratorSetting(Constants.AGE_OFF_ITERATOR_PRIORITY, "ageoff", AgeOffFilter.class);
		ageOffIteratorSetting.addOption("ttl", "" + ageOffTimeInMilliseconds);
		return ageOffIteratorSetting;
	}

	/**
	 * Returns an {@link IteratorSetting} that specifies the combiner of {@link SetOfStatistics}.
	 *
	 * @return An IteratorSetting applying the set of statistics combiner iterator
	 */
	public static IteratorSetting getSetOfStatisticsCombinerIteratorSetting() {
		IteratorSetting iteratorSetting = new IteratorSetting(Constants.SET_OF_STATISTICS_COMBINER_PRIORITY,
				"SetOfStatisticsCombiner", SetOfStatisticsCombiner.class);
		iteratorSetting.addOption("all", "true");
		return iteratorSetting;
	}

	/**
	 * Returns an {@link IteratorSetting} that can be used to apply the
	 * {@link RawElementWithStatsFilter} iterator to either a {@link Scanner}
	 * or an {@link InputFormat}.
	 *
	 * @param predicate  The predicate to apply
	 * @return An IteratorSetting applying the given predicate
	 */
	public static IteratorSetting getPreRollUpFilterIteratorSetting(Predicate<RawGraphElementWithStatistics> predicate) {
		IteratorSetting iteratorSetting = new IteratorSetting(Constants.PRE_ROLLUP_FILTER_PRIORITY,
				Constants.RAW_ELEMENT_WITH_STATS_PREDICATE_FILTER_NAME, RawElementWithStatsFilter.class);
		try {
			iteratorSetting.addOption(Constants.RAW_ELEMENT_WITH_STATS_PREDICATE, WritableToStringConverter.serialiseToString(predicate));
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to serialise RawGraphElementWithStatisticsPredicate to string: " + e);
		}
		return iteratorSetting;
	}

	/**
	 * Returns an {@link IteratorSetting} that can be used to apply the {@link StatisticTransformIterator} to
	 * either a {@link Scanner} or an {@link InputFormat}.
	 *
	 * @param transform  The StatisticsTransform to apply
	 * @return An IteratorSetting applying the given StatisticsTransform
	 */
	public static IteratorSetting getStatisticTransformIteratorSetting(StatisticsTransform transform) {
		IteratorSetting iteratorSetting = new IteratorSetting(Constants.STATISTICS_TRANSFORM_PRIORITY,
				Constants.STATISTIC_TRANSFORM_ITERATOR_NAME, StatisticTransformIterator.class);
		try {
			iteratorSetting.addOption(Constants.STATISTIC_TRANSFORM, WritableToStringConverter.serialiseToString(transform));
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to serialise StatisticsTransform to string: " + transform);
		}
		return iteratorSetting;
	}

	/**
	 * Returns an {@link IteratorSetting} that can be used to apply the
	 * {@link RollUpOverTimeAndVisibility} iterator to either a {@link Scanner}
	 * or an {@link InputFormat}.
	 *
	 * @return An IteratorSetting for rolling up over time and visibility
	 */
	public static IteratorSetting getRollUpOverTimeAndVisibilityIteratorSetting() {
		IteratorSetting iteratorSetting = new IteratorSetting(Constants.ROLL_UP_OVER_TIME_AND_VISIBILITY_COMBINER_PRIORITY,
				"rollupovervisandtime", RollUpOverTimeAndVisibility.class);
		return iteratorSetting;
	}

}
