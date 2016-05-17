/**
 * Copyright 2016 Crown Copyright
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

package gaffer.spark

import org.apache.accumulo.core.client.AccumuloException
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import gaffer.accumulostore.AccumuloStore
import gaffer.data.element.{Element, Properties}
import gaffer.accumulostore.utils.Pair

import scala.collection.JavaConversions._
import scala.collection.SortedMap

/** Represents a Gaffer table in Spark. Can be queried to return [[RDD]]s of [[Element]]s and [[Properties]].
  */
class GafferTableSc (protected val store: AccumuloStore, val sc: SparkContext) {

  val config = new GraphConfig(store)

  protected def query(conf: Configuration, useBatchInputFormat: Boolean) = {
    if (useBatchInputFormat) {
      sc.newAPIHadoopRDD(new Configuration(conf), classOf[BatchScannerElementInputFormat], classOf[Element], classOf[Properties])
    } else {
      sc.newAPIHadoopRDD(new Configuration(conf), classOf[ElementInputFormat], classOf[Element], classOf[Properties])
    }
  }

  /** Creates an [[RDD]] by querying the Gaffer table for all its members.
    *
    * @return An [[RDD]] of [[Element]], [[Properties]] pairs.
    */
  def query: RDD[(Element, Properties)] = {
    val conf = new Configuration
    config.setConfiguration(conf)
    query(conf, useBatchInputFormat = false)
  }

  /** Creates an [[RDD]] by querying the table for an [[Iterable]] of [[Object]] vertices.
    *
    * Note that [[Edge]]s may be returned twice if both ends are in the [[Iterable]] queried for.
    *
    * @param vertices The [[Object]] vertices to be queried for.
    * @return An [[RDD]] of [[Element]], [[Properties]] pairs.
    */
  def query(vertices: Iterable[Object]): RDD[(Element, Properties)] = {
    val conf = new Configuration
    config.setConfiguration(conf, vertices)
    query(conf, useBatchInputFormat = true)
  }

  /** Creates an [[RDD]] by querying the table for a single [[Object]] vertex.
    *
    * @param vertex The [[Object]] vertex to be queried for.
    * @return An [[RDD]] of [[Element]], [[Properties]] pairs.
    */
  def query(vertex: Object): RDD[(Element, Properties)] = query(Iterable(vertex))

  /** Creates an [[RDD]] by querying the table for an [[Iterable]] of ranges made up from [[Pair]]s.
    *
    * @param ranges The [[Pair]]s to be queried for.
    * @return An [[RDD]] of [[Element]], [[Properties]] pairs.
    */
  def rangeQuery(ranges: Iterable[Pair[Object]]): RDD[(Element, Properties)] = {
    val conf = new Configuration()
    config.setConfigurationFromRanges(conf, ranges)
    query(conf, useBatchInputFormat = true)
  }

  /** Creates an [[RDD]] by querying the table for a range made up from a [[Pair]].
    *
    * @param range The [[Pair]] to be queried for.
    * @return An [[RDD]] of [[Element]], [[Properties]] pairs.
    */
  def rangeQuery(range: Pair[Object]): RDD[(Element, Properties)] = rangeQuery(Iterable(range))

  /** Creates an [[RDD]] by querying the table for an [[Iterable]] of pairs of [[Object]] vertices.
    *
    * Only those edges between the two entities will be returned.
    *
    * @param pairs The pairs of [[Object]]s to be queried for.
    * @return An [[RDD]] of [[Element]], [[Properties]] pairs.
    */
  def pairQuery(pairs: Iterable[(Object, Object)]): RDD[(Element, Properties)] = {
    val conf = new Configuration()  
    config.setConfigurationFromPairs(conf, pairs map (p => new Pair[Object](p._1, p._2)))
    query(conf, useBatchInputFormat = true)
  }

  /** Creates an [[RDD]] by querying the table for a pair of [[Object]] vertices.
    *
    * Only those edges between the two entities will be returned.
    *
    * @param pair The pair of [[Object]]s to be queried for.
    * @return An [[RDD]] of [[Element]], [[Properties]] pairs.
    */
  def pairQuery(pair: (Object, Object)): RDD[(Element, Properties)] = pairQuery(Iterable(pair))

  /** Creates a new [[GafferTable]] which only contains the [[Entity]]s from the base table.
    *
    * @return The new [[GafferTable]].
    */
  def onlyEntities: this.type = {
    config.setReturnEntitiesOnly()
    this
  }

  /** Creates a new [[GafferTable]] which only contains the [[Edge]]s from the base table.
    *
    * @return The new [[GafferTable]].
    */
  def onlyEdges: this.type = {
    config.setReturnEdgesOnly()
    this
  }

  /** Creates a new [[GafferTable]] that contains both the [[Edge]]s and [[Entity]] from the base table.
    *
    * @return The new [[GafferTable]].
    */
  def entitiesAndEdges: this.type = {
    config.setReturnEntitiesAndEdges()
    this
  }

  /** Creates a new [[GafferTable]] which only contains those [[Element]]s from the base table whose [[Comparable]]
	 *  value is at or after the first value and at or before the second value.
	 *  The values relate to a given property [[String]].
	 *
	 * @param prop The property relating to the range of values.
	 * @param start The start value of the range to be queried.
	 * @param end The end value of the range to be queried.
	 * @return The new [[GafferTable]].
	 */
  def between(prop: String, start: Comparable[_], end: Comparable[_]): this.type = {
    config.setRange(prop, start, end)
    this
  }

  /** Creates a new [[GafferTable]] which only contains those [[Element]]s from the base table whose [[Comparable]]
	 *  value is at or before the value supplied. The value relates to a given property [[String]].
	 *
	 * @param prop The property relating to the range of values.
	 * @param end The end value of the range to be queried.
	 * @return The new [[GafferTable]].
	 */
  def before(prop: String, end: Comparable[_]): this.type = {
    config.setBefore(prop, end)
    this
  }

  /** Creates a new [[GafferTable]] which only contains those [[Element]]s from the base table whose [[Comparable]]
	 *  value is at or after the value supplied. The value relates to a given property [[String]].
	 *
	 * @param prop The property relating to the range of values.
	 * @param start The start value of the range to be queried.
	 * @return The new [[GafferTable]].
	 */
  def after(prop: String, start: Comparable[_]): this.type = {
    config.setAfter(prop, start)
    this
  }
  
    /** Creates a new [[GafferTable]] which rolls up the [[Element]]s it returns over different time
    * windows.
    *
    * Note that this is the default behaviour for a newly created [[GafferTable]].
    *
    * @return The new [[GafferTable]].
    */
  def withRollup: this.type = {
    config.rollUpOverTimeAndVisibility(true)
    this
  }

  /** Creates a new [[GafferTable]] which returns distinct [[Element]]s for different time windows.
    *
    * @return The new [[GafferTable]].
    */
  def withoutRollup: this.type = {
    config.rollUpOverTimeAndVisibility(false)
    this
  }

	/** Creates a new [[GafferTable]] containing those [[Element]]s from the base table whose group
	 *  is of the group supplied.
	 *
	 * @param group The group to be included.
	 * @return The new [[GafferTable]].
	 */
  def onlyGroups(groups: Set[String]): this.type = {
    config.setGroupTypes(groups)
    this
  }

	/** Creates a new [[GafferTable]] containing those [[Element]]s from the base table whose groups
	 * are in the given {@link Set}.
	 *
	 * @param groups The groups to be included.
	 * @return The new [[GafferTable]].
	 */
  def onlyGroups(group: String*): this.type =
    onlyGroups(group.toSet)

  /**
   * Clones this [[GafferTable]].
   * @return The cloned [[GafferTable]]
   */
  def cloneTable(): GafferTableSc = new GafferTableSc(store, sc)

}



