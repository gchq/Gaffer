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
package gaffer.graph.wrappers;

import gaffer.graph.Edge;
import gaffer.statistics.SetOfStatistics;

import java.io.Serializable;

/**
 * A simple class that wraps an {@link Edge} and a {@link SetOfStatistics}.
 */
public class EdgeWithStatistics implements Serializable {

	private static final long serialVersionUID = 6714924545121692701L;
	private Edge edge;
	private SetOfStatistics setOfStatistics;
	
	public EdgeWithStatistics() { }
	
	public EdgeWithStatistics(Edge edge, SetOfStatistics setOfStatistics) {
		this.edge = edge;
		this.setOfStatistics = setOfStatistics;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((edge == null) ? 0 : edge.hashCode());
		result = prime * result
				+ ((setOfStatistics == null) ? 0 : setOfStatistics.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EdgeWithStatistics other = (EdgeWithStatistics) obj;
		if (edge == null) {
			if (other.edge != null)
				return false;
		} else if (!edge.equals(other.edge))
			return false;
		if (setOfStatistics == null) {
			if (other.setOfStatistics != null)
				return false;
		} else if (!setOfStatistics.equals(other.setOfStatistics))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return edge + " " + setOfStatistics;
	}

	public Edge getEdge() {
		return edge;
	}

	public void setEdge(Edge edge) {
		this.edge = edge;
	}

	public SetOfStatistics getSetOfStatistics() {
		return setOfStatistics;
	}

	public void setSetOfStatistics(SetOfStatistics setOfStatistics) {
		this.setOfStatistics = setOfStatistics;
	}

}
