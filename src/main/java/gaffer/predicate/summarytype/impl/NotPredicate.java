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
package gaffer.predicate.summarytype.impl;

import gaffer.predicate.summarytype.SummaryTypePredicate;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
* Inverts a given {@link SummaryTypePredicate}.
*/
public class NotPredicate extends SummaryTypePredicate {

	private static final long serialVersionUID = -813807685626592712L;
	private SummaryTypePredicate predicate;

	public NotPredicate() { }

	public NotPredicate(SummaryTypePredicate predicate) {
		this.predicate = predicate;
	}

	@Override
	public boolean accept(String summaryType, String summarySubType) {
		return !predicate.accept(summaryType, summarySubType);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, predicate.getClass().getName());
		predicate.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			String predicate1ClassName = Text.readString(in);
			predicate = (SummaryTypePredicate) Class.forName(predicate1ClassName).newInstance();
			predicate.readFields(in);
		} catch (InstantiationException e) {
			throw new IOException("Unable to deserialise NotPredicate: " + e);
		} catch (IllegalAccessException e) {
			throw new IOException("Unable to deserialise NotPredicate: " + e);
		} catch (ClassNotFoundException e) {
			throw new IOException("Unable to deserialise NotPredicate: " + e);
		} catch (ClassCastException e) {
			throw new IOException("Unable to deserialise NotPredicate: " + e);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((predicate == null) ? 0 : predicate.hashCode());
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
		NotPredicate other = (NotPredicate) obj;
		if (predicate == null) {
			if (other.predicate != null)
				return false;
		} else if (!predicate.equals(other.predicate))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "NotPredicate [predicate=" + predicate + "]";
	}

}
