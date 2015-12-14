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
package gaffer.predicate.typevalue.impl;

import gaffer.predicate.typevalue.TypeValuePredicate;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
* Creates a combination of two {@link TypeValuePredicate}s: they can either
* be ANDed together, ORed together or XORed together.
*/
public class CombinedPredicates implements TypeValuePredicate {

	private static final long serialVersionUID = -5395342971280794512L;

	public enum Combine {AND, OR, XOR}

	private TypeValuePredicate predicate1;
	private TypeValuePredicate predicate2;
	private Combine combine;

	public CombinedPredicates() { }

	public CombinedPredicates(TypeValuePredicate predicate1,
							  TypeValuePredicate predicate2, Combine combine) {
		this.predicate1 = predicate1;
		this.predicate2 = predicate2;
		this.combine = combine;
	}

	@Override
	public boolean accept(String type, String value) {
		switch (combine) {
		case AND:
			return predicate1.accept(type, value)
					&& predicate2.accept(type, value);
		case OR:
			return predicate1.accept(type, value)
					|| predicate2.accept(type, value);
		case XOR:
			return predicate1.accept(type, value)
					^ predicate2.accept(type, value);
		default:
			return false;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, predicate1.getClass().getName());
		predicate1.write(out);
		Text.writeString(out, predicate2.getClass().getName());
		predicate2.write(out);
		out.writeInt(combine.ordinal());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			String predicate1ClassName = Text.readString(in);
			predicate1 = (TypeValuePredicate) Class.forName(predicate1ClassName).newInstance();
			predicate1.readFields(in);
			String predicate2ClassName = Text.readString(in);
			predicate2 = (TypeValuePredicate) Class.forName(predicate2ClassName).newInstance();
			predicate2.readFields(in);
		} catch (InstantiationException e) {
			throw new IOException("Unable to deserialise CombinedPredicates: " + e);
		} catch (IllegalAccessException e) {
			throw new IOException("Unable to deserialise CombinedPredicates: " + e);
		} catch (ClassNotFoundException e) {
			throw new IOException("Unable to deserialise CombinedPredicates: " + e);
		} catch (ClassCastException e) {
			throw new IOException("Unable to deserialise CombinedPredicates: " + e);
		}
		combine = Combine.values()[in.readInt()];
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((combine == null) ? 0 : combine.hashCode());
		result = prime * result
				+ ((predicate1 == null) ? 0 : predicate1.hashCode());
		result = prime * result
				+ ((predicate2 == null) ? 0 : predicate2.hashCode());
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
		CombinedPredicates other = (CombinedPredicates) obj;
		if (combine != other.combine)
			return false;
		if (predicate1 == null) {
			if (other.predicate1 != null)
				return false;
		} else if (!predicate1.equals(other.predicate1))
			return false;
		if (predicate2 == null) {
			if (other.predicate2 != null)
				return false;
		} else if (!predicate2.equals(other.predicate2))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CombinedPredicates{" +
				"predicate1=" + predicate1 +
				", predicate2=" + predicate2 +
				", combine=" + combine +
				'}';
	}
}
