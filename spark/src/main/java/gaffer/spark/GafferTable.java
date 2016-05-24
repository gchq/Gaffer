/*
 * Copyright 2016 Crown Copyright
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

package gaffer.spark;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;

import gaffer.accumulostore.utils.Pair;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.exception.RangeFactoryException;
import gaffer.data.element.Element;
import gaffer.data.element.Properties;
import scala.Tuple2;

public class GafferTable {

    protected JavaSparkContext sc;
    protected GraphConfig config;
    protected AccumuloStore store;

    /**
     * Represents a Gaffer table in Spark. Can be queried to return
     * {@link JavaPairRDD}s of {@link Element}s and their {@link Properties}.
     * @param store The {@link AccumuloStore} to be queried.
     * @param sc Spark's {@link JavaSparkContext}.
     */
    protected GafferTable(final AccumuloStore store, final JavaSparkContext sc) {
        this.store = store;
        this.sc = sc;
        config = new GraphConfig(this.store);

    }

    protected JavaPairRDD<Element, Properties> query(final Configuration conf, final boolean useBatchInputFormat) {
        if (useBatchInputFormat) {
            return sc.newAPIHadoopRDD(new Configuration(conf), BatchScannerElementInputFormat.class, Element.class,
                    Properties.class);
        } else {
            return sc.newAPIHadoopRDD(new Configuration(conf), ElementInputFormat.class, Element.class,
                    Properties.class);
        }
    }

    /**
     * Creates a {@link JavaPairRDD} by querying the Gaffer table for all its
     * members.
     *
     * @return A {@link JavaPairRDD} of {@link Element}, {@link Properties}
     *         pairs.
     */
    public JavaPairRDD<Element, Properties> query() {
        Configuration conf = new Configuration();
        config.setConfiguration(conf);
        return query(conf, false);
    }

    /**
     * Creates a {@link JavaPairRDD} by querying the table for a
     * {@link Collection} of {@link Object} vertices.
     *
     * Note that edges may be returned twice if both ends are in the
     * {@link Collection} queried for.
     *
     * @param vertices
     *            The {@link Object} vertices to be queried for.
     * @return A {@link JavaPairRDD} of {@link Element}, {@link Properties}
     *         pairs.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public JavaPairRDD<Element, Properties> query(final Collection<Object> vertices) throws RangeFactoryException {
        Configuration conf = new Configuration();
        config.setConfiguration(conf, vertices);
        return query(conf, true);
    }

    /**
     * Creates a {@link JavaPairRDD} by querying the table for a single
     * {@link Object} vertex.
     *
     * @param vertex
     *            The {@link Object} to be queried for.
     * @return A {@link JavaPairRDD} of {@link Element}, {@link Properties}
     *         pairs.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public JavaPairRDD<Element, Properties> query(final Object vertex) throws RangeFactoryException {
        return query(Collections.singleton(vertex));
    }

    /**
     * Creates a {@link JavaPairRDD} by querying the table for a
     * {@link Collection} of ranges from {@link Pair}s of vertices.
     *
     * @param ranges
     *            The {@link Pair}s of vertices to be queried for.
     * @return A {@link JavaPairRDD} of {@link Element}, {@link Properties}
     *         pairs.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public JavaPairRDD<Element, Properties> rangeQuery(final Collection<Pair<Object>> ranges) throws RangeFactoryException {
        Configuration conf = new Configuration();
        config.setConfigurationFromRanges(conf, ranges);
        return query(conf, true);
    }

    /**
     * Creates a {@link JavaPairRDD} by querying the table for a range from a
     * {@link Pair} of vertices.
     *
     * @param range
     *            The {@link Pair} of vertices to be queried for.
     * @return a {@link JavaPairRDD} of {@link Element}, {@link Properties}
     *         pairs.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public JavaPairRDD<Element, Properties> rangeQuery(final Pair<Object> range) throws RangeFactoryException {
        return rangeQuery(Collections.singleton(range));
    }

    /**
     * Creates a {@link JavaPairRDD} by querying the table for an
     * {@link Collection} of pairs of {@link Object} vertices.
     *
     * Only those edges between the two entities will be returned.
     *
     * @param pairs
     *            The pairs of {@link Object} vertices to be queried for.
     * @return a {@link JavaPairRDD} of {@link Element}, {@link Properties}
     *         pairs.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public JavaPairRDD<Element, Properties> pairQuery(final Collection<Tuple2<Object, Object>> pairs) throws RangeFactoryException {
        Configuration conf = new Configuration();

        Collection<Pair<Object>> coll = new HashSet<>();
        for (Tuple2<Object, Object> t : pairs) {
            coll.add(new Pair<>(t._1(), t._2()));
        }
        config.setConfigurationFromPairs(conf, coll);
        return query(conf, true);
    }

    /**
     * Creates a {@link JavaPairRDD} by querying the table for a pair of
     * {@link Object} vertices.
     *
     * Only those edges between the two entities will be returned.
     *
     * @param pair
     *            The pair of {@link Object} vertices to be queried for.
     * @return a {@link JavaPairRDD} of {@link Element}, {@link Properties}
     *         pairs.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public JavaPairRDD<Element, Properties> pairQuery(final Tuple2<Object, Object> pair) throws RangeFactoryException {
        return pairQuery(Collections.singleton(pair));
    }

    /**
     * Creates a new {@link GafferTable} which only contains the entities
     * from the base table.
     *
     * @return The new {@link GafferTable}.
     */
    public GafferTable onlyEntities() {
        GafferTable gt = this.cloneTable();
        gt.config.setReturnEntitiesOnly();
        return gt;
    }

    /**
     * Creates a new {@link GafferTable} which only contains the edges
     * from the base table.
     *
     * @return The new {@link GafferTable}.
     */
    public GafferTable onlyEdges() {
        GafferTable gt = this.cloneTable();
        gt.config.setReturnEdgesOnly();
        return gt;
    }

    /**
     * Creates a new {@link GafferTable} that contains both the edges
     * and entities from the base table.
     *
     * @return The new {@link GafferTable}.
     */
    public GafferTable entitiesAndEdges() {
        GafferTable gt = this.cloneTable();
        gt.config.setReturnEntitiesAndEdges();
        return gt;
    }

    /**
     * Creates a new {@link GafferTable} which only contains those
     * {@link Element}s from the base table whose {@link Comparable} value is at
     * or after the first value and at or before the second value. The values
     * relate to a given property {@link String}.
     *
     * @param prop
     *            The property relating to the range of values.
     * @param start
     *            The start value of the range to be queried.
     * @param end
     *            The end value of the range to be queried.
     * @return The new {@link GafferTable}.
     */
    public GafferTable between(final String prop, final Comparable start, final Comparable end) {
        GafferTable gt = this.cloneTable();
        gt.config.setRange(prop, start, end);
        return gt;
    }

    /**
     * Creates a new {@link GafferTable} which only contains those
     * {@link Element}s from the base table whose {@link Comparable} value is at
     * or before the value supplied. The value relates to a given property
     * {@link String}.
     *
     * @param prop
     *            The property relating to the range of values.
     * @param end
     *            The end value of the range to be queried.
     * @return The new {@link GafferTable}.
     */
    public GafferTable before(final String prop, final Comparable end) {
        GafferTable gt = this.cloneTable();
        gt.config.setBefore(prop, end);
        return gt;
    }

    /**
     * Creates a new {@link GafferTable} which only contains those
     * {@link Element}s from the base table whose {@link Comparable} value is at
     * or after the value supplied. The value relates to a given property
     * {@link String}.
     *
     * @param prop
     *            The property relating to the range of values.
     * @param start
     *            The start value of the range to be queried.
     * @return The new {@link GafferTable}.
     */
    public GafferTable after(final String prop, final Comparable start) {
        GafferTable gt = this.cloneTable();
        gt.config.setAfter(prop, start);
        return gt;
    }

    /**
     * Creates a new {@link GafferTable} which rolls up the {@link Element}s it
     * returns using the summarise function.
     *
     * Note that this is the default behaviour for a newly created
     * {@link GafferTable}.
     *
     * @return The new {@link GafferTable}.
     */
    public GafferTable withRollup() {
        GafferTable gt = this.cloneTable();
        gt.config.summarise(true);
        return gt;
    }

    /**
     * Creates a new {@link GafferTable} which returns distinct {@link Element}s
     * without summarising.
     *
     * @return The new {@link GafferTable}.
     */
    public GafferTable withoutRollup() {
        GafferTable gt = this.cloneTable();
        gt.config.summarise(false);
        return gt;
    }

    /**
     * Creates a new {@link GafferTable} containing those {@link Element}s from
     * the base table whose group is of the group supplied.
     *
     * @param group
     *            The group to be included.
     * @return The new {@link GafferTable}.
     */
    public GafferTable onlyGroups(final String group) {
        return onlyGroups(Collections.singleton(group));
    }

    /**
     * Creates a new {@link GafferTable} containing those {@link Element}s from
     * the base table whose groups are in the given {@link Set}.
     *
     * @param groups
     *            The groups to be included.
     * @return The new {@link GafferTable}.
     */
    public GafferTable onlyGroups(final Set<String> groups) {
        GafferTable gt = this.cloneTable();
        gt.config.setGroupTypes(groups);
        return gt;
    }

    /**
     * Clones this {@link GafferTable}.
     *
     * @return The cloned {@link GafferTable}
     */
    public GafferTable cloneTable() {
        return new GafferTable(store, sc);
    }

}
