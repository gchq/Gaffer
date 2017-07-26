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

package uk.gov.gchq.gaffer.spark.operation.graphx

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.spark.graphx.Graph
import uk.gov.gchq.gaffer.data.element.{Edge, Entity}
import uk.gov.gchq.gaffer.operation.Operation
import uk.gov.gchq.gaffer.operation.io.InputOutput

class PageRank extends Operation with InputOutput[Graph[Entity, Edge], Graph[Double, Double]] {

  private var input: Graph[Entity, Edge] = null
  private var tolerance: Double = 0.001

  def getTolerance(): Double = tolerance

  def setTolerance(tol: Double) = tolerance = tol

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
  override def getInput: Graph[Entity, Edge] = input

  override def setInput(graph: Graph[Entity, Edge]): Unit = input = graph // cannot reassign to val

  override def getOutputTypeReference: TypeReference[Graph[Double, Double]] = new TypeReference[Graph[Double, Double]] {}
}

object PageRank {
  class Builder() extends Operation.BaseBuilder[PageRank, PageRank.Builder](new PageRank) with InputOutput.Builder[PageRank, Graph[Entity, Edge], Graph[Double, Double], PageRank.Builder] {}
}

