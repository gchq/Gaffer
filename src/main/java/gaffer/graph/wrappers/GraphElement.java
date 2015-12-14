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
import gaffer.graph.Entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * A wrapper for an {@link Edge} or an {@link Entity}. Needed so that MapReduce jobs can
 * use a GraphElement as a key.
 */
public class GraphElement implements WritableComparable<GraphElement>, Serializable {

	// Register the raw comparator
	static {
		WritableComparator.define(GraphElement.class, new GraphElementComparator());
	}

	private static final long serialVersionUID = -6885603026826195547L;

	private Entity entity;
	private Edge edge;
	private boolean isEntity; // true if contains an Entity, false if contains an Edge

	public GraphElement() {
		this.entity = new Entity();
		this.edge = new Edge();
	}

	public GraphElement(Entity entity) {
		this.isEntity = true;
		this.entity = entity;
		this.edge = new Edge();
	}

	public GraphElement(Edge edge) {
		this.isEntity = false;
		this.entity = new Entity();
		this.edge = edge;
	}

	public boolean isEntity() {
		return isEntity;
	}

	public boolean isEdge() {
		return !isEntity;
	}

	public Entity getEntity() {
		if (isEntity) {
			return entity;
		}
		return null;
	}

	public Edge getEdge() {
		if (!isEntity) {
			return edge;
		}
		return null;
	}

	public String getSummaryType() {
		if (isEntity) {
			return entity.getSummaryType();
		} else {
			return edge.getSummaryType();
		}
	}

	public String getSummarySubType() {
		if (isEntity) {
			return entity.getSummarySubType();
		} else {
			return edge.getSummarySubType();
		}
	}

	public Date getStartDate() {
		if (isEntity) {
			return entity.getStartDate();
		} else {
			return edge.getStartDate();
		}
	}

	public Date getEndDate() {
		if (isEntity) {
			return entity.getEndDate();
		} else {
			return edge.getEndDate();
		}
	}

	public String getVisibility() {
		if (isEntity) {
			return entity.getVisibility();
		} else {
			return edge.getVisibility();
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		isEntity = in.readBoolean();
		if (isEntity) {
			entity.readFields(in);
		} else {
			edge.readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isEntity);
		if (isEntity) {
			entity.write(out);
		} else {
			edge.write(out);
		}
	}

	@Override
	public int compareTo(GraphElement o) {
		if (this.equals(o)) {
			return 0;
		}
		if (isEntity && o.isEntity) {
			return entity.compareTo(o.getEntity());
		}
		if (!isEntity && !o.isEntity) {
			return edge.compareTo(o.getEdge());
		}
		// We're comparing one Entity and one Edge.
		// IF type-value in Entity is the same as source type-value in the Edge THEN Entity comes first.
		// IF type-value in Entity is less than the source type-value in the Edge THEN Entity comes first.
		// IF type-value in Entity is greater than the source type-value in the Edge THEN Entity comes second.
		if (isEntity) {
			// This is an Entity and o is an Edge
			int compareType = entity.getEntityType().compareTo(o.getEdge().getSourceType());
			int compareValue = entity.getEntityValue().compareTo(o.getEdge().getSourceValue());
			if (compareType == 0 && compareValue == 0) {
				return -1;
			}
			if (compareType < 0 || (compareType == 0 && compareValue < 0)) {
				return -1;
			} else {
				// Must have compareType > 0 || (compareType == 0 && compareValue > 0)
				return 1;
			}
		} else {
			// This is an Edge and o is an Entity
			int compareType = edge.getSourceType().compareTo(o.getEntity().getEntityType());
			int compareValue = edge.getSourceValue().compareTo(o.getEntity().getEntityValue());
			if (compareType == 0 && compareValue == 0) {
				return 1;
			}
			if (compareType < 0 || (compareType == 0 && compareValue < 0)) {
				return -1;
			} else {
				// Must have compareType > 0 || (compareType == 0 && compareValue > 0)
				return 1;
			}
		}
	}

	@Override
	public int hashCode() {
		if (isEntity) {
			return entity.hashCode();
		} else {
			return edge.hashCode();
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GraphElement other = (GraphElement) obj;
		if (isEntity != other.isEntity) {
			return false;
		}
		if (isEntity) {
			return entity.equals(other.getEntity());
		} else {
			return edge.equals(other.getEdge());
		}
	}

	@Override
	public String toString() {
		if (isEntity) {
			return "Entity = " + entity.toString();
		} else {
			return "Edge = " + edge.toString();
		}
	}
	
	public GraphElement clone() {
		if (isEntity) {
			return new GraphElement(entity.clone());
		}
		return new GraphElement(edge.clone());
	}

	public static class GraphElementComparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		private static final Entity.EntityComparator ENTITY_COMPARATOR = new Entity.EntityComparator();
		private static final Edge.EdgeComparator EDGE_COMPARATOR = new Edge.EdgeComparator();

		public GraphElementComparator() {
			super(GraphElement.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				boolean firstIsEntity = b1[s1] == 1;
				boolean secondIsEntity = b2[s2] == 1;
				s1++;
				l1--;
				s2++;
				l2--;
				// If both entities, then delegate to Entity's raw comparator
				if (firstIsEntity && secondIsEntity) {
					return ENTITY_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
				}
				// If both edges, then delegate to Edge's raw comparator
				if (!firstIsEntity && !secondIsEntity) {
					return EDGE_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
				}
				// We're comparing one Entity and one Edge.
				// IF type-value in Entity is the same as source type-value in the Edge THEN Entity comes first.
				// IF type-value in Entity is less than the source type-value in the Edge THEN Entity comes first.
				// IF type-value in Entity is greater than the source type-value in the Edge THEN Entity comes second.
				// Compare type then value
				int firstLength = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int secondLength = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int compare = TEXT_COMPARATOR.compare(b1, s1, firstLength, b2, s2, secondLength);
				if (compare < 0) {
					return -1;
				} else if (compare > 0) {
					return 1;
				}
				s1 += firstLength;
				s2 += secondLength;
				firstLength = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				secondLength = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				compare = TEXT_COMPARATOR.compare(b1, s1, firstLength, b2, s2, secondLength);
				if (compare == 0) {
					return -1;
				}
				return compare;
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

}
