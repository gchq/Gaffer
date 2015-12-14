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

import gaffer.Pair;
import gaffer.predicate.summarytype.SummaryTypePredicate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

/**
 * A predicate that returns true if the given summary type and subType is in the
 * provided set of {@link Pair}s of {@link String}s.
 */
public class SummaryTypeAndSubTypeInSetPredicate extends SummaryTypePredicate {

	private static final long serialVersionUID = -7724136863820485456L;
	private Set<Pair<String>> allowedSummaryTypesAndSubTypes;

	public SummaryTypeAndSubTypeInSetPredicate() {
		this.allowedSummaryTypesAndSubTypes = new HashSet<Pair<String>>();
	}

	public SummaryTypeAndSubTypeInSetPredicate(Set<Pair<String>> allowedSummaryTypesAndSubTypes) {
		this.allowedSummaryTypesAndSubTypes = allowedSummaryTypesAndSubTypes;
	}

	public SummaryTypeAndSubTypeInSetPredicate(Pair<String>... allowedSummaryTypesAndSubTypes) {
		this();
		Collections.addAll(this.allowedSummaryTypesAndSubTypes, allowedSummaryTypesAndSubTypes);
	}

	public void addAllowedSummaryTypes(Set<Pair<String>> allowedSummaryTypesAndSubTypes) {
		this.allowedSummaryTypesAndSubTypes.addAll(allowedSummaryTypesAndSubTypes);
	}

	public boolean accept(String summaryType, String summarySubType) {
		Pair<String> pair = new Pair<String>(summaryType, summarySubType);
		return allowedSummaryTypesAndSubTypes.contains(pair);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		allowedSummaryTypesAndSubTypes = new HashSet<Pair<String>>(size);
		for (int i = 0; i < size; i++) {
			allowedSummaryTypesAndSubTypes.add(new Pair<String>(Text.readString(in), Text.readString(in)));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(allowedSummaryTypesAndSubTypes.size());
		for (Pair<String> pair : allowedSummaryTypesAndSubTypes) {
			Text.writeString(out, pair.getFirst());
			Text.writeString(out, pair.getSecond());
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((allowedSummaryTypesAndSubTypes == null) ? 0
						: allowedSummaryTypesAndSubTypes.hashCode());
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
		SummaryTypeAndSubTypeInSetPredicate other = (SummaryTypeAndSubTypeInSetPredicate) obj;
		if (allowedSummaryTypesAndSubTypes == null) {
			if (other.allowedSummaryTypesAndSubTypes != null)
				return false;
		} else if (!allowedSummaryTypesAndSubTypes
				.equals(other.allowedSummaryTypesAndSubTypes))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SummaryTypeAndSubTypeInSetPredicate [allowedSummaryTypesAndSubTypes="
				+ allowedSummaryTypesAndSubTypes + "]";
	}

}
