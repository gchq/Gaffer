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

package uk.gov.gchq.gaffer.spark.data.graphx

import org.apache.spark.graphx
import uk.gov.gchq.gaffer.data.element.Edge

case class GafferEdge(edge: Edge) {

  def getSource(): Object = {
    edge.getSource
  }

  def getDestination(): Object = {
    edge.getDestination
  }

  def toGraphX(): graphx.Edge[Edge] = {
    graphx.Edge(edge.getSource.hashCode(), edge.getDestination.hashCode())
  }

  object GafferEdge {

    def apply(edge: Edge): graphx.Edge[Edge] =
      new GafferEdge(edge).toGraphX()
  }
}
