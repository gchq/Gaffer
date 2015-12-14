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
 * An entity - represents a summarised set of information about a node.
 * Each node has a type and a value. These types and values must be non-null
 * and must not be the empty string.
 * 
 * The entity has a start date and an end date - the entity summarises
 * information about the type-value from within this time period. Typically
 * when this is used in applications the start and end dates of the entity
 * will correspond to the start and end of a day, but any time window is
 * possible. The start and the end date must not be null.
 * 
 * An entity has a summary type and subtype. When there are both entities
 * and {@link Edge}s in Gaffer, then it is typical to include an Entity for
 * each of the endpoints of the {@link Edge}s, and for the summary type
 * and subtype of the entity to match that of the edge.
 * 
 * An entity has a visibility. Typically, interactions with a certain
 * visibility are combined into an entity with that visibility, so we
 * might have an "public" entity summarising the publicly viewable interactions
 * and a "private" entity summarising the interactions that only be be seen by certain
 * individuals.
 */
public class Entity implements WritableComparable<Entity>, Serializable {

	// Register the raw comparator
	static {
		WritableComparator.define(Entity.class, new EntityComparator());
	}

	private static final long serialVersionUID = -8534335348915318788L;

	private static DateFormat UTC_FORMAT = DateFormat.getDateTimeInstance();
	static {
		UTC_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}
	
	private String entityType;
	private String entityValue;
	private String summaryType;
	private String summarySubType;
	private String visibility;
	private Date startDate = new Date();
	private Date endDate = new Date();

	/**
	 * Standard constructor.
	 * 
	 * @param entityType  The type of the entity - must be non-null and not the empty string
	 * @param entityValue  The value of the entity - must be non-null and not the empty string
	 * @param summaryType  The type of information about the entity that is being summarised - must be non-null but can be the empty string
	 * @param summarySubType  The subType of the information about the entity that is being summarised - must be non-null but can be the empty string
	 * @param visibility  The visibility of the edge
	 * @param startDate  The start of the time period that the entity is summarising - must be non-null
	 * @param endDate  The end of the time period that the entity is summarising - must be non-null
	 */
	public Entity(String entityType,
			String entityValue,
			String summaryType,
			String summarySubType,
			String visibility,
			Date startDate,
			Date endDate) {
		// Validate arguments
		validate(entityType, entityValue, summaryType, summarySubType, startDate, endDate);
		// Set arguments
		this.entityType = entityType;
		this.entityValue = entityValue;
		this.summaryType = summaryType;
		this.summarySubType = summarySubType;
		this.visibility = visibility;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	/**
	 * Standard no-args constructor.
	 */
	public Entity() {}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.entityType = Text.readString(in);
		this.entityValue = Text.readString(in);
		this.summaryType = Text.readString(in);
		this.summarySubType = Text.readString(in);
		this.visibility = Text.readString(in);
		this.startDate.setTime(in.readLong());
		this.endDate.setTime(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, entityType);
		Text.writeString(out, entityValue);
		Text.writeString(out, summaryType);
		Text.writeString(out, summarySubType);
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
		result = prime * result + ((endDate == null) ? 0 : endDate.hashCode());
		result = prime * result
				+ ((entityType == null) ? 0 : entityType.hashCode());
		result = prime * result
				+ ((entityValue == null) ? 0 : entityValue.hashCode());
		result = prime
				* result
				+ ((summarySubType == null) ? 0 : summarySubType
						.hashCode());
		result = prime * result
				+ ((summaryType == null) ? 0 : summaryType.hashCode());
		result = prime * result
				+ ((startDate == null) ? 0 : startDate.hashCode());
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
		Entity other = (Entity) obj;
		if (visibility == null) {
			if (other.visibility != null)
				return false;
		} else if (!visibility.equals(other.visibility))
			return false;
		if (endDate == null) {
			if (other.endDate != null)
				return false;
		} else if (!endDate.equals(other.endDate))
			return false;
		if (entityType == null) {
			if (other.entityType != null)
				return false;
		} else if (!entityType.equals(other.entityType))
			return false;
		if (entityValue == null) {
			if (other.entityValue != null)
				return false;
		} else if (!entityValue.equals(other.entityValue))
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
		if (startDate == null) {
			if (other.startDate != null)
				return false;
		} else if (!startDate.equals(other.startDate))
			return false;
		return true;
	}

	public int compareTo(Entity o) {
		if (!this.entityType.equals(o.entityType)) {
			return this.entityType.compareTo(o.entityType);
		} else if (!this.entityValue.equals(o.entityValue)) {
			return this.entityValue.compareTo(o.entityValue);
		} else if (!this.summaryType.equals(o.summaryType)) {
			return this.summaryType.compareTo(o.summaryType);
		} else if (!this.summarySubType.equals(o.summarySubType)) {
			return this.summarySubType.compareTo(o.summarySubType);
		} else if (this.visibility != o.visibility) {
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
		return "Entity [entityType=" + entityType + ", entityValue="
				+ entityValue + ", summaryType=" + summaryType
				+ ", summarySubType=" + summarySubType
				+ ", visibility=" + visibility + ", startDate="
				+ UTC_FORMAT.format(startDate) + ", endDate=" + UTC_FORMAT.format(endDate) + "]";
	}

	public Entity clone() {
		return new Entity(entityType, entityValue, summaryType, summarySubType, visibility, new Date(startDate.getTime()), new Date(endDate.getTime()));
	}

	public String getEntityType() {
		return entityType;
	}

	public void setEntityType(String entityType) {
		validate(entityType, this.entityValue, this.summaryType, this.summarySubType, this.startDate, this.endDate);
		this.entityType = entityType;
	}

	public String getEntityValue() {
		return entityValue;
	}

	public void setEntityValue(String entityValue) {
		validate(this.entityType, entityValue, this.summaryType, this.summarySubType, this.startDate, this.endDate);
		this.entityValue = entityValue;
	}

	public TypeValue getSimpleEntity() {
		return new TypeValue(entityType, entityValue);
	}

	public String getSummaryType() {
		return summaryType;
	}

	public void setSummaryType(String summaryType) {
		validate(this.entityType, this.entityValue, summaryType, this.summarySubType, this.startDate, this.endDate);
		this.summaryType = summaryType;
	}

	public String getSummarySubType() {
		return summarySubType;
	}

	public void setSummarySubType(String summarySubType) {
		validate(this.entityType, this.entityValue, this.summaryType, summarySubType, this.startDate, this.endDate);
		this.summarySubType = summarySubType;
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

	private void validate(String entityType, String entityValue, String summaryType, String summarySubType, Date startDate, Date endDate) {
		if (entityType == null || entityType.length() == 0) {
			throw new IllegalArgumentException("Invalid entityType - must be non-null and of non-zero length");
		}
		if (entityValue == null || entityValue.length() == 0) {
			throw new IllegalArgumentException("Invalid entityValue - must be non-null and of non-zero length");
		}
		if (summaryType == null) {
			throw new IllegalArgumentException("Invalid summaryType - must be non-null");
		}
		if (summarySubType == null) {
			throw new IllegalArgumentException("Invalid summarySubType - must be non-null");
		}
		if (startDate == null) {
			throw new IllegalArgumentException("Invalid entity startDate - must be non-null");
		}
		if (endDate == null) {
			throw new IllegalArgumentException("Invalid entity endDate - must be non-null");
		}
		if (startDate.after(endDate)) {
			throw new IllegalArgumentException("Invalid entity dates - startDate must not be after endDate");
		}
	}

	public static class EntityComparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public EntityComparator() {
			super(Entity.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				// Compare entity type, then value, then summary type, then subtype, then visibility
				for (int i = 0; i < 5; i++) {
					int firstLength = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
					int secondLength = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
					int compare = TEXT_COMPARATOR.compare(b1, s1, firstLength, b2, s2, secondLength);
					if (compare != 0) {
						return compare;
					}
					s1 += firstLength;
					s2 += secondLength;
				}
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
