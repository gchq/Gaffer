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

package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler

import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import uk.gov.gchq.gaffer.data.element
import uk.gov.gchq.gaffer.spark.operation.graphx.RddToGraphX
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler
import uk.gov.gchq.gaffer.store.{Context, Store}

class RddToGraphXHandler extends OutputOperationHandler[RddToGraphX, Graph[_, _]] {

  override def doOperation(operation: RddToGraphX, context: Context, store: Store): Graph[_, _] = {

    val edges: RDD[element.Edge] = operation.getInput.filter(e => e.isInstanceOf[element.Edge]).map(e => e.asInstanceOf[element.Edge])
    val entities: RDD[element.Entity] = operation.getInput.filter(e => e.isInstanceOf[element.Entity]).map(e => e.asInstanceOf[element.Entity])

    val graphxVertices: VertexRDD[element.Entity] = VertexRDD(entities.map(e => (e.getVertex.hashCode(), e)))
    val graphxEdges = edges.map(e => Edge(e.getSource.hashCode(), e.getDestination.hashCode(), e))

    Graph(graphxVertices, graphxEdges)
  }
}
