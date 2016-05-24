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

package gaffer.spark

import gaffer.accumulostore.AccumuloStore
import gaffer.accumulostore.utils.Pair
import gaffer.data.element.{Edge, Element, Entity, Properties}
import gaffer.store.Store

import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, PasswordToken}
import org.apache.accumulo.core.security.{Authorizations, SystemPermission}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConversions._

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class GafferTableSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = _
  var table: GafferTableSc = _
  var store: Store = _ 
  var data: GafferTableData = _

  override def beforeAll(configMap: ConfigMap): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    data = new GafferTableData()
    store = data.getStore()
    sc = new SparkContext(new SparkConf().setAppName("gafferscalatest").setMaster("local").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryo.registrator", "gaffer.spark.GafferRegistrator"))
    table = new GafferTableSc(store.asInstanceOf[AccumuloStore], sc)
  }

  override def afterAll(configMap: ConfigMap): Unit = {
    sc.stop()
    System.clearProperty("spark.master.port")
    data.stopCluster()
  }

  "A GafferTable" should "return the entire table when query() is called" in {
    table.query.collect.toSet should equal(data.expectedOutput.toSet)
  }

  it should "return only entities when it is called with onlyEntities set" in {
    table.onlyEntities.query.collect.toSet should equal(data.entityExpectedOutput.toSet)
  }

  it should "return only edges when it is called with onlyEdges set" in {
    table.onlyEdges.query.collect.toSet should equal(data.edgeExpectedOutput.toSet)
  }

  it should "revert to returning the whole table when entitiesAndEdges is set" in {
    table.onlyEdges.entitiesAndEdges.query.collect.toSet should equal(data.expectedOutput.toSet)
  }

  it should "default to using rollup" in {
    table.query.collect should equal(table.withRollup.query.collect)
  }

  it should "return elements without rollup when withoutRollup is set" in {
    table.withoutRollup.query.collect.toSet should equal(data.expectedUnrolledOutput.toSet)
  }

  it should "revert to using rollup when withRollup is set" in {
    table.withoutRollup.withRollup.query.collect.toSet should equal(data.expectedOutput.toSet)
  }

  def matchesEntity(vertex: Object)(el: Element): Boolean = {
    if (el.isInstanceOf[Edge]) {
      val edge = el.asInstanceOf[Edge]
      edge.getSource == vertex || edge.getDestination == vertex
    } else {
      val entity = el.asInstanceOf[Entity]
      entity.getVertex == vertex
    }
  }

  it should "return all entities and edges related to a given Vertex" in {
    val firstEntity = "filmA"
    val firstEntityResults = data.expectedOutput.toSet filter (p => matchesEntity(firstEntity)(p._1))
    table.query(firstEntity).collect.toSet should equal(firstEntityResults)

    val secondEntity = "filmB"
    val secondEntityResults = data.expectedOutput.toSet filter (p => matchesEntity(secondEntity)(p._1))

    table.query(Iterable(firstEntity, secondEntity)).collect.toSet should equal(firstEntityResults ++ secondEntityResults)
  }

  def inRange(vertex1: String)(vertex2: String)(el: Element) = {
    if (el.isInstanceOf[Edge]) {
      val edge = el.asInstanceOf[Edge]
      val source = edge.getSource.asInstanceOf[String]
      val dest = edge.getDestination.asInstanceOf[String]
      ((source > vertex1 || source == vertex1) &&
        (source < vertex2 || source == vertex2)) ||
        ((dest > vertex1 || dest == vertex1) &&
          (dest < vertex2 || dest == vertex2))
    } else {
      val entity = el.asInstanceOf[Entity]
      val vertex = entity.getVertex.asInstanceOf[String]
      (vertex > vertex1 || vertex == vertex1) &&
        (vertex < vertex2 || vertex == vertex2)
    }
  }

  it should "return all entities and edges within a given range" in {
    val vertex1 = "user01"
    val vertex2 = "user02"

    val rangeResults = data.expectedOutput.toSet filter (p => inRange(vertex1)(vertex2)(p._1))

    table.rangeQuery(new Pair[Object](vertex1, vertex2)).collect.toSet should equal(rangeResults)
  }

  it should "return only those edges corresponding to a given pair" in {
    val value1 = "filmC"
    val value2 = "user02"

    val pairResults = data.expectedOutput.toSet filter (p => matchesEntity(value1)(p._1) && matchesEntity(value2)(p._1))

    table.pairQuery(value1, value2).collect.toSet should equal(pairResults)

    val value3 = "filmA"
    val value4 = "user01"

    val pair2Results = data.expectedOutput.toSet filter (p => matchesEntity(value3)(p._1) && matchesEntity(value4)(p._1))

    table.pairQuery(Iterable((value1, value2), (value3, value4))).collect.toSet should equal(pairResults ++ pair2Results)
  }

  it should "return only the elements corresponding to a given group" in {
    val groupResults = data.expectedOutput.toSet filter (p => p._1.getGroup == "user")
    table.onlyGroups("user").query.collect.toSet should equal(groupResults)

    val group2Results = data.expectedOutput.toSet filter (p => p._1.getGroup == "review")
    table.onlyGroups(Set("user", "review")).query.collect.toSet should equal(groupResults ++ group2Results)
  }

  it should "return only those elements within a given range of dates" in {
    val prop = "startTime"
    val rangeStart = 1401000000000L
    val rangeEnd = 1408000000000L
    val inRangeResults = data.expectedOutput.toSet filter (p => (p._2.containsKey(prop)) &&
      (p._2.get(prop).asInstanceOf[Long] > rangeStart) &&
        (p._2.get(prop).asInstanceOf[Long] < rangeEnd)
      )

    table.between(prop, rangeStart, rangeEnd).query.collect.toSet should equal(inRangeResults)
  }

  it should "return only those elements after a given date" in {
    val prop = "startTime"
    val rangeStart = 1402000000000L
    val afterDateResults = data.expectedOutput.toSet filter (p => p._2.containsKey(prop) && 
        p._2.get(prop).asInstanceOf[Long] > rangeStart)

    table.after(prop, rangeStart).query.collect.toSet should equal(afterDateResults)
  }

  it should "return only those elements before a given date" in {
    val prop = "startTime"
    val rangeEnd = 1408000000000L
    val untilDateResults = data.expectedOutput.toSet filter (p => p._2.containsKey(prop) && 
        p._2.get(prop).asInstanceOf[Long] < rangeEnd) 
    
    table.before(prop, rangeEnd).query.collect.toSet should equal(untilDateResults)
  }
}