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
import gaffer.analytic.Analytic;
import gaffer.graph.TypeValueRange;

/**
 * Parameters to store a set of {@link TypeValueRange}s for use in an
 * {@link Analytic}.
 */
public class TypeValueRangeSet extends AccumuloParameters {

	private Iterable<TypeValueRange> typeValueRanges;

	public TypeValueRangeSet() { }

	public TypeValueRangeSet(AccumuloBackedGraph accumuloGraph, Iterable<TypeValueRange> typeValueRanges) {
		super.setAccumuloGraph(accumuloGraph);
		this.typeValueRanges = typeValueRanges;
	}
	
	public Iterable<TypeValueRange> getTypeValueRanges() {
		return typeValueRanges;
	}

	public void setTypeValueRanges(Iterable<TypeValueRange> typeValueRanges) {
		this.typeValueRanges = typeValueRanges;
	}
	
}
