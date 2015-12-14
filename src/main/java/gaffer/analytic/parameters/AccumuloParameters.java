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
package gaffer.analytic.parameters;

import gaffer.accumulo.AccumuloBackedGraph;

/**
 * To be used as base class for analytics that require access to
 * an {@link AccumuloBackedGraph}.
 */
public class AccumuloParameters implements Parameters {

	private AccumuloBackedGraph accumuloGraph;
	
	public AccumuloParameters() { }
	
	public AccumuloParameters(AccumuloBackedGraph accumuloGraph) {
		this.accumuloGraph = accumuloGraph;
	}

	public AccumuloBackedGraph getAccumuloGraph() {
		return accumuloGraph;
	}

	public void setAccumuloGraph(AccumuloBackedGraph accumuloGraph) {
		this.accumuloGraph = accumuloGraph;
	}

}
