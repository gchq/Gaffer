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

import gaffer.statistics.SetOfStatistics;

import java.io.Serializable;
import java.util.Date;

/**
 * A simple class that wraps a {@link GraphElement} and a {@link SetOfStatistics}.
 */
public class GraphElementWithStatistics implements Serializable {

	private static final long serialVersionUID = -2231682455554520275L;
	private GraphElement graphElement;
	private SetOfStatistics setOfStatistics;
	
	public GraphElementWithStatistics() { }
	
	public GraphElementWithStatistics(GraphElement graphElement, SetOfStatistics setOfStatistics) {
		this.graphElement = graphElement;
		this.setOfStatistics = setOfStatistics;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((graphElement == null) ? 0 : graphElement.hashCode());
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
		GraphElementWithStatistics other = (GraphElementWithStatistics) obj;
		if (graphElement == null) {
			if (other.graphElement != null)
				return false;
		} else if (!graphElement.equals(other.graphElement))
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
		return graphElement + ", " + setOfStatistics;
	}

	public GraphElement getGraphElement() {
		return graphElement;
	}

	public void setGraphElement(GraphElement graphElement) {
		this.graphElement = graphElement;
	}

	public String getEntityType() {
		if (!isEntity()) {
			throw new UnsupportedOperationException("Cannot access the entity type of a GraphElementWithStatistics that contains an Edge: " + toString());
		}
		return graphElement.getEntity().getEntityType();
	}

	public String getEntityValue() {
		if (!isEntity()) {
			throw new UnsupportedOperationException("Cannot access the entity value of a GraphElementWithStatistics that contains an Edge: " + toString());
		}
		return graphElement.getEntity().getEntityValue();
	}

	public String getSourceType() {
		if (!isEdge()) {
			throw new UnsupportedOperationException("Cannot access the source type of a GraphElementWithStatistics that contains an Entity: " + toString());
		}
		return graphElement.getEdge().getSourceType();
	}

	public String getSourceValue() {
		if (!isEdge()) {
			throw new UnsupportedOperationException("Cannot access the source value of a GraphElementWithStatistics that contains an Entity: " + toString());
		}
		return graphElement.getEdge().getSourceValue();
	}

	public String getDestinationType() {
		if (!isEdge()) {
			throw new UnsupportedOperationException("Cannot access the destination type of a GraphElementWithStatistics that contains an Entity: " + toString());
		}
		return graphElement.getEdge().getDestinationType();
	}

	public String getDestinationValue() {
		if (!isEdge()) {
			throw new UnsupportedOperationException("Cannot access the destination value of a GraphElementWithStatistics that contains an Entity: " + toString());
		}
		return graphElement.getEdge().getDestinationValue();
	}

	public boolean isDirected() {
		if (!isEdge()) {
			throw new UnsupportedOperationException("Cannot ask whether a GraphElementWithStatistics that is an Entity is directed: " + toString());
		}
		return graphElement.getEdge().isDirected();
	}

	public SetOfStatistics getSetOfStatistics() {
		return setOfStatistics;
	}

	public void setSetOfStatistics(SetOfStatistics setOfStatistics) {
		this.setOfStatistics = setOfStatistics;
	}

	public String getSummaryType() {
		return graphElement.getSummaryType();
	}

	public String getSummarySubType() {
		return graphElement.getSummarySubType();
	}

	public Date getStartDate() {
		return graphElement.getStartDate();
	}

	public Date getEndDate() {
		return graphElement.getEndDate();
	}

	public String getVisibility() {
		return graphElement.getVisibility();
	}

	public boolean isEntity() {
		return graphElement.isEntity();
	}

	public boolean isEdge() {
		return !graphElement.isEntity();
	}

	public GraphElementWithStatistics clone() {
		return new GraphElementWithStatistics(graphElement.clone(), setOfStatistics.clone());
	}

}
