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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

/**
 * A predicate that returns true if the given summary type is in the
 * provided set of {@link String}s.
 */
public class SummaryTypeInSetPredicate extends SummaryTypePredicate {

	private static final long serialVersionUID = 2445811708688453414L;
	private Set<String> allowedSummaryTypes;

	public SummaryTypeInSetPredicate() {
		this.allowedSummaryTypes = new HashSet<String>();
	}

	public SummaryTypeInSetPredicate(Set<String> allowedSummaryTypes) {
		this.allowedSummaryTypes = allowedSummaryTypes;
	}

	public SummaryTypeInSetPredicate(String... allowedSummaryTypes) {
		this();
		Collections.addAll(this.allowedSummaryTypes, allowedSummaryTypes);
	}

	public void addAllowedSummaryTypes(Set<String> allowedSummaryTypes) {
		this.allowedSummaryTypes.addAll(allowedSummaryTypes);
	}

	public void addAllowedSummaryTypes(String... allowedSummaryTypes) {
		Collections.addAll(this.allowedSummaryTypes, allowedSummaryTypes);
	}

	public boolean accept(String summaryType, String summarySubType) {
		return allowedSummaryTypes.contains(summaryType);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		allowedSummaryTypes = new HashSet<String>(size);
		for (int i = 0; i < size; i++) {
			allowedSummaryTypes.add(Text.readString(in));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(allowedSummaryTypes.size());
		for (String s : allowedSummaryTypes) {
			Text.writeString(out, s);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((allowedSummaryTypes == null) ? 0 : allowedSummaryTypes
						.hashCode());
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
		SummaryTypeInSetPredicate other = (SummaryTypeInSetPredicate) obj;
		if (allowedSummaryTypes == null) {
			if (other.allowedSummaryTypes != null)
				return false;
		} else if (!allowedSummaryTypes.equals(other.allowedSummaryTypes))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SummaryTypeInSetPredicate [allowedSummaryTypes="
				+ allowedSummaryTypes + "]";
	}

}
