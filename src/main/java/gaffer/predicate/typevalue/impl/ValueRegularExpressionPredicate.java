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
import java.util.regex.Pattern;

/**
 * A {@link TypeValuePredicate} that indicates whether the value matches the provided regular
 * expression.
 */
public class ValueRegularExpressionPredicate implements TypeValuePredicate {

    private static final long serialVersionUID = -7642698064068014288L;
    private Pattern pattern;

    public ValueRegularExpressionPredicate() { }

    public ValueRegularExpressionPredicate(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public boolean accept(String type, String value) {
        return pattern.matcher(value).find();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, pattern.pattern());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pattern = Pattern.compile(Text.readString(in));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValueRegularExpressionPredicate that = (ValueRegularExpressionPredicate) o;

        if (pattern != null ? !pattern.pattern().equals(that.pattern.pattern()) : that.pattern.pattern() != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return pattern != null ? pattern.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ValueRegularExpressionPredicate{" +
                "pattern=" + pattern +
                '}';
    }
}
