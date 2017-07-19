/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.graphx

import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.scalactic.TolerantNumerics
import uk.gov.gchq.gaffer.commonutil.spec.UnitSpec
import uk.gov.gchq.gaffer.data.element.{Edge, Element, Entity}
import uk.gov.gchq.gaffer.spark.operation.graphx.PageRank
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.PageRankHandler

class RddToGraphXHandlerSpec extends UnitSpec {

  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  "A PageRank operation" should "create a PageRank graph" in {
    // Given
    val graph = iterableToGraphX(createSimpleGraph())

    val pageRank = new PageRank
    pageRank.setInput(graph)

    val handler = new PageRankHandler

    val result = handler.doOperation(pageRank, null, null).vertices.map(f => f._2).collect().sorted

    result shouldBe sorted
    result should contain allOf(0.15, 0.78, 1.48, 1.57)

    result.foreach(println)
  }

  def createSimpleGraph(): Iterable[Element] = {
    val pageA = new Entity.Builder().group("page").vertex("A").build()
    val pageB = new Entity.Builder().group("page").vertex("B").build()
    val pageC = new Entity.Builder().group("page").vertex("C").build()
    val pageD = new Entity.Builder().group("page").vertex("D").build()

    val edge1 = new Edge.Builder().directed(true).group("edge").source("A").dest("B").build();
    val edge2 = new Edge.Builder().directed(true).group("edge").source("A").dest("C").build();
    val edge3 = new Edge.Builder().directed(true).group("edge").source("B").dest("C").build();
    val edge4 = new Edge.Builder().directed(true).group("edge").source("C").dest("A").build();
    val edge5 = new Edge.Builder().directed(true).group("edge").source("D").dest("C").build();

    List(pageA, pageB, pageC, pageD, edge1, edge2, edge3, edge4, edge5)
  }

  def iterableToGraphX(iter: Iterable[Element]): Graph[Entity, Edge] = {
    val edges: Iterable[Edge] = iter.filter(e => e.isInstanceOf[Edge]).map(e => e.asInstanceOf[Edge])
    val entities: Iterable[Entity] = iter.filter(e => e.isInstanceOf[Entity]).map(e => e.asInstanceOf[Entity])

    val conf = new SparkConf().setMaster("local[2]").setAppName("PageRankHandlerSpec")
    val sc = new SparkContext(conf)

    val graphxVertices: VertexRDD[Entity] = VertexRDD(sc.parallelize(entities.toSeq).map(e => (e.getVertex.hashCode(), e)))
    val graphxEdges = sc.parallelize(edges.map(e => new graphx.Edge(e.getSource.hashCode(), e.getDestination.hashCode(), e)).toSeq)

    Graph(graphxVertices, graphxEdges)
  }

}
