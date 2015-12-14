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
package gaffer.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * An edge - represents a summarised set of interactions between two nodes.
 * Each node has a type and a value. These types and values must be non-null
 * and must not be the empty string.
 * 
 * The edge has a start date and an end date - the edge summarises interactions
 * that happen within this time period. Typically when this is used in bulk graph
 * applications the start and end dates of the edges will correspond to the start
 * and end of a day, but any time window is possible. The start and the end date
 * must not be null.
 * 
 * An edge has a summary type and a subtype, this is intended to reflect the
 * type of information that is being summarised.
 *
 * Note that it is not necessary to create any {@link Entity}s when creating
 * {@link Edge}s and vice-versa.
 * 
 * An edge can either be directed or undirected. If it is directed then the
 * edge goes from the source to the destination.
 * 
 * An edge has a visibility. Typically, interactions with a certain
 * visibility are combined into an edge with that visibility, so we
 * might have an "public" edge summarising the publicly viewable interactions
 * and a "private" edge summarising the interactions that can only be be seen
 * by certain individuals.
 * 
 * An edge is allowed to be a self-edge, i.e. the source and destination can be
 * identical.
 */
public class Edge implements WritableComparable<Edge>, Serializable {

	// Register the raw comparator
	static {
		WritableComparator.define(Edge.class, new EdgeComparator());
	}

	private static final long serialVersionUID = -6560186628114287093L;

	private static DateFormat UTC_FORMAT = DateFormat.getDateTimeInstance();
	static {
		UTC_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	private String sourceType;
	private String sourceValue;
	private String destinationType;
	private String destinationValue;
	private String summaryType;
	private String summarySubType;
	private boolean directed; // If true then this is a directed edge from the source 
	// to the destination. If false then this is an undirected edge.
	private String visibility;
	private Date startDate = new Date();
	private Date endDate = new Date();

	/**
	 * Standard constructor.
	 * 
	 * @param sourceType  The type of the source of the edge - must be non-null and not the empty string
	 * @param sourceValue  The value of the source of the edge - must be non-null and not the empty string
	 * @param destinationType  The type of the destination of the edge - must be non-null and not the empty string
	 * @param destinationValue  The value of the destination of the edge - must be non-null and not the empty string
	 * @param summaryType  The type of the edge - must be non-null but can be the empty string
	 * @param summarySubType  The subtype of the edge - must be non-null but can be the empty string
	 * @param directed  If true then indicates that this is a directed edge from the source to the destination
	 * @param visibility  The visibility of the edge
	 * @param startDate  The start of the time period that the edge is summarising - must be non-null
	 * @param endDate  The end of the time period that the edge is summarising - must be non-null
	 */
	public Edge(String sourceType,
			String sourceValue,
			String destinationType,
			String destinationValue,
			String summaryType,
			String summarySubType,
			boolean directed,
			String visibility,
			Date startDate,
			Date endDate) {
		// Validate arguments
		validate(sourceType, sourceValue, destinationType, destinationValue, summaryType, summarySubType, startDate, endDate);
		// Set arguments
		this.summaryType = summaryType;
		this.summarySubType = summarySubType;
		this.directed = directed;
		this.visibility = visibility;
		this.startDate = startDate;
		this.endDate = endDate;
		this.sourceType = sourceType;
		this.sourceValue = sourceValue;
		this.destinationType = destinationType;
		this.destinationValue = destinationValue;
		reorder();
	}

	/**
	 * Standard no-args constructor.
	 */
	public Edge() {	}

	/**
	 * Returns the opposite end of the edge to the provided {@link TypeValue}. If the provided
	 * {@link TypeValue} is not one of the ends of the edge then an {@link IllegalArgumentException}
	 * is thrown.
	 *
	 * @param typeValue  A TypeValue that should be at one end of the edge
	 * @return The TypeValue at the other end of the edge
	 */
	public TypeValue getOtherEnd(TypeValue typeValue) {
		if (sourceType.equals(typeValue.getType()) && sourceValue.equals(typeValue.getValue())) {
			return new TypeValue(destinationType, destinationValue);
		}
		if (destinationType.equals(typeValue.getType()) && destinationValue.equals(typeValue.getValue())) {
			return new TypeValue(sourceType, sourceValue);
		}
		throw new IllegalArgumentException("The provided TypeValue (" + typeValue + ") is not one of the ends of the edge");
	}

	public void readFields(DataInput in) throws IOException {
		this.sourceType = Text.readString(in);
		this.sourceValue = Text.readString(in);
		this.destinationType = Text.readString(in);
		this.destinationValue = Text.readString(in);
		this.summaryType = Text.readString(in);
		this.summarySubType = Text.readString(in);
		this.directed = in.readBoolean();
		this.visibility = Text.readString(in);
		this.startDate.setTime(in.readLong());
		this.endDate.setTime(in.readLong());
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, sourceType);
		Text.writeString(out, sourceValue);
		Text.writeString(out, destinationType);
		Text.writeString(out, destinationValue);
		Text.writeString(out, summaryType);
		Text.writeString(out, summarySubType);
		out.writeBoolean(directed);
		Text.writeString(out, visibility);
		out.writeLong(startDate.getTime());
		out.writeLong(endDate.getTime());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((visibility == null) ? 0 : visibility.hashCode());
		result = prime * result
				+ ((destinationType == null) ? 0 : destinationType.hashCode());
		result = prime
				* result
				+ ((destinationValue == null) ? 0 : destinationValue.hashCode());
		result = prime * result + ((endDate == null) ? 0 : endDate.hashCode());
		result = prime * result + (directed ? 1231 : 1237);
		result = prime * result
				+ ((sourceType == null) ? 0 : sourceType.hashCode());
		result = prime * result
				+ ((sourceValue == null) ? 0 : sourceValue.hashCode());
		result = prime * result
				+ ((startDate == null) ? 0 : startDate.hashCode());
		result = prime * result + ((summarySubType == null) ? 0 : summarySubType.hashCode());
		result = prime * result + ((summaryType == null) ? 0 : summaryType.hashCode());
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
		Edge other = (Edge) obj;
		if (visibility == null) {
			if (other.visibility != null)
				return false;
		} else if (!visibility.equals(other.visibility))
			return false;
		if (destinationType == null) {
			if (other.destinationType != null)
				return false;
		} else if (!destinationType.equals(other.destinationType))
			return false;
		if (destinationValue == null) {
			if (other.destinationValue != null)
				return false;
		} else if (!destinationValue.equals(other.destinationValue))
			return false;
		if (endDate == null) {
			if (other.endDate != null)
				return false;
		} else if (!endDate.equals(other.endDate))
			return false;
		if (directed != other.directed)
			return false;
		if (sourceType == null) {
			if (other.sourceType != null)
				return false;
		} else if (!sourceType.equals(other.sourceType))
			return false;
		if (sourceValue == null) {
			if (other.sourceValue != null)
				return false;
		} else if (!sourceValue.equals(other.sourceValue))
			return false;
		if (startDate == null) {
			if (other.startDate != null)
				return false;
		} else if (!startDate.equals(other.startDate))
			return false;
		if (summarySubType == null) {
			if (other.summarySubType != null)
				return false;
		} else if (!summarySubType.equals(other.summarySubType))
			return false;
		if (summaryType == null) {
			if (other.summaryType != null)
				return false;
		} else if (!summaryType.equals(other.summaryType))
			return false;
		return true;
	}

	public int compareTo(Edge o) {
		if (!this.sourceType.equals(o.sourceType)) {
			return this.sourceType.compareTo(o.sourceType);
		} else if (!this.sourceValue.equals(o.sourceValue)) {
			return this.sourceValue.compareTo(o.sourceValue);
		} else if (!this.destinationType.equals(o.destinationType)) {
			return this.destinationType.compareTo(o.destinationType);
		} else if (!this.destinationValue.equals(o.destinationValue)) {
			return this.destinationValue.compareTo(o.destinationValue);
		} else if (!this.summaryType.equals(o.summaryType)) {
			return this.summaryType.compareTo(o.summaryType);
		} else if (!this.summarySubType.equals(o.summarySubType)) {
			return this.summarySubType.compareTo(o.summarySubType);
		} else if (this.directed != o.directed) {
			// Want undirected first
			return this.directed ? 1 : -1;
		} else if (!this.visibility.equals(o.visibility)) {
			return this.visibility.compareTo(o.visibility);
		} else if (!this.startDate.equals(o.startDate)) {
			return this.startDate.compareTo(o.startDate);
		} else if (!this.endDate.equals(o.endDate)) {
			return this.endDate.compareTo(o.endDate);
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return "sourceType=" + sourceType + ", sourceValue="
				+ sourceValue + ", destinationType=" + destinationType
				+ ", destinationValue=" + destinationValue + ", summaryType=" + summaryType
				+ ", summarySubType=" + summarySubType + ", directed="
				+ directed + ", visibility=" + visibility +
				", startDate=" + UTC_FORMAT.format(startDate) + ", endDate=" + UTC_FORMAT.format(endDate);
	}

	public Edge clone() {
		return new Edge(sourceType, sourceValue, destinationType, destinationValue, summaryType, summarySubType, directed, visibility, new Date(startDate.getTime()), new Date(endDate.getTime()));
	}

	// Getters - NB don't include individual setters for type and value
	// as they get reordered internally.

	public String getSourceType() {
		return sourceType;
	}

	public String getSourceValue() {
		return sourceValue;
	}

	public TypeValue getSourceAsTypeValue() {
		return new TypeValue(sourceType, sourceValue);
	}

	public void setSourceTypeAndValue(String sourceType, String sourceValue) {
		this.sourceType = sourceType;
		this.sourceValue = sourceValue;
		reorder();
	}

	public String getDestinationType() {
		return destinationType;
	}

	public String getDestinationValue() {
		return destinationValue;
	}

	public TypeValue getDestinationAsTypeValue() {
		return new TypeValue(destinationType, destinationValue);
	}

	public void setDestinationTypeAndValue(String destinationType, String destinationValue) {
		this.destinationType = destinationType;
		this.destinationValue = destinationValue;
		reorder();
	}

	public String getSummaryType() {
		return summaryType;
	}

	public void setSummaryType(String type) {
		this.summaryType = type;
	}

	public String getSummarySubType() {
		return summarySubType;
	}

	public void setSummarySubType(String summarySubType) {
		this.summarySubType = summarySubType;
	}

	public boolean isDirected() {
		return directed;
	}

	public void setDirected(boolean directed) {
		this.directed = directed;
	}

	public String getVisibility() {
		return visibility;
	}

	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	private void validate(String sourceType, String sourceValue, String destinationType, String destinationValue,
			String type, String subType, Date startDate, Date endDate) {
		if (sourceType == null || sourceType.length() == 0) {
			throw new IllegalArgumentException("Invalid sourceType - must be non-null and of non-zero length");
		}
		if (sourceValue == null || sourceValue.length() == 0) {
			throw new IllegalArgumentException("Invalid sourceValue - must be non-null and of non-zero length");
		}
		if (destinationType == null || destinationType.length() == 0) {
			throw new IllegalArgumentException("Invalid destinationType - must be non-null and of non-zero length");
		}
		if (destinationValue == null || destinationValue.length() == 0) {
			throw new IllegalArgumentException("Invalid destinationValue - must be non-null and of non-zero length");
		}
		if (type == null) {
			throw new IllegalArgumentException("Invalid edge type - must be non-null");
		}
		if (subType == null) {
			throw new IllegalArgumentException("Invalid edge subType - must be non-null");
		}
		if (startDate == null) {
			throw new IllegalArgumentException("Invalid edge startDate - must be non-null");
		}
		if (endDate == null) {
			throw new IllegalArgumentException("Invalid edge endDate - must be non-null");
		}
		if (startDate.after(endDate)) {
			throw new IllegalArgumentException("Invalid edge dates - startDate must not be after endDate");
		}
	}

	/**
	 * If edge is undirected then we order the two type-values - the source should be less than
	 * the destination (comparing type first then value). 
	 */
	private void reorder() {
		if (directed) {
			return;
		}
		// If edge is undirected then order the two type-values
		// - the source should be less than the destination
		// (comparing type first then value).
		if (sourceType.compareTo(destinationType) < 0) {
			return;
		} else if (sourceType.compareTo(destinationType) > 0) {
			// Flip the source and destination
			String temp = destinationType;
			destinationType = sourceType;
			sourceType = temp;
			temp = destinationValue;
			destinationValue = sourceValue;
			sourceValue = temp;
		} else {
			// Types are equal, so compare the values.
			if (sourceValue.compareTo(destinationValue) <= 0) {
				return;
			} else {
				String temp = destinationValue;
				destinationValue = sourceValue;
				sourceValue = temp;
			}
		}
	}

	public static class EdgeComparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public EdgeComparator() {
			super(Edge.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				// Compare source type, then source value, then destination type, then destination value,
				// then summary type, then summary subtype
				for (int i = 0; i < 6; i++) {
					int firstLength = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
					int secondLength = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
					int compare = TEXT_COMPARATOR.compare(b1, s1, firstLength, b2, s2, secondLength);
					if (compare != 0) {
						return compare;
					}
					s1 += firstLength;
					s2 += secondLength;
				}
				// Compare directed - if not the same then want undirected first
				if (b1[s1] != b2[s2]) {
					if (b1[s1] == 0) {
						return -1;
					} else {
						return 1;
					}
				}
				s1++;
				s2++;
				// Compare visibility
				int firstLength = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int secondLength = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int compare = TEXT_COMPARATOR.compare(b1, s1, firstLength, b2, s2, secondLength);
				if (compare != 0) {
					return compare;
				}
				s1 += firstLength;
				s2 += secondLength;
				// Compare start and end dates
				for (int i = 0; i < 16; i++) {
					if (b1[s1 + i] != b2[s2 + i]) {
						return b1[s1 + i] - b2[s2 + i];
					}
				}
				return 0;
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}

		}
	}

}
