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
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

/**
 * This predicate will return true if the summary type and subtype match
 * the supplied {@link Pattern}s.
 */
public class RegularExpressionPredicate extends SummaryTypePredicate {

	private static final long serialVersionUID = -4479947452468396893L;
	private Pattern summaryTypePattern;
	private Pattern summarySubTypePattern;

	public RegularExpressionPredicate() { }
	
	public RegularExpressionPredicate(Pattern summaryTypePattern,
			Pattern summarySubTypePattern) {
		this.summaryTypePattern = summaryTypePattern;
		this.summarySubTypePattern = summarySubTypePattern;
	}

	@Override
	public boolean accept(String summaryType, String summarySubType) {
		return summaryTypePattern.matcher(summaryType).find()
				&& summarySubTypePattern.matcher(summarySubType).find();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, summaryTypePattern.pattern());
		Text.writeString(out, summarySubTypePattern.pattern());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		summaryTypePattern = Pattern.compile(Text.readString(in));
		summarySubTypePattern = Pattern.compile(Text.readString(in));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((summarySubTypePattern == null) ? 0 : summarySubTypePattern
						.hashCode());
		result = prime
				* result
				+ ((summaryTypePattern == null) ? 0 : summaryTypePattern
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
		RegularExpressionPredicate other = (RegularExpressionPredicate) obj;
		if (summarySubTypePattern == null) {
			if (other.summarySubTypePattern != null)
				return false;
		} else if (!summarySubTypePattern.pattern().equals(other.summarySubTypePattern.pattern()))
			return false;
		if (summaryTypePattern == null) {
			if (other.summaryTypePattern != null)
				return false;
		} else if (!summaryTypePattern.pattern().equals(other.summaryTypePattern.pattern()))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RegularExpressionPredicate [summaryTypePattern="
				+ summaryTypePattern + ", summarySubTypePattern="
				+ summarySubTypePattern + "]";
	}

}
