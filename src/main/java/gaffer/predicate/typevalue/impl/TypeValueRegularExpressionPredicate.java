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
 * A {@link TypeValuePredicate} that indicates whether the type and value match the respective regular
 * expressions provided.
 */
public class TypeValueRegularExpressionPredicate implements TypeValuePredicate {

    private static final long serialVersionUID = -1234598064068016543L;
    private Pattern typePattern;
    private Pattern valuePattern;

    public TypeValueRegularExpressionPredicate() { }

    public TypeValueRegularExpressionPredicate(Pattern typePattern, Pattern valuePattern) {
        this.typePattern = typePattern;
        this.valuePattern = valuePattern;
    }

    @Override
    public boolean accept(String type, String value) {
        return typePattern.matcher(type).find() && valuePattern.matcher(value).find();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, typePattern.pattern());
        Text.writeString(out, valuePattern.pattern());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        typePattern = Pattern.compile(Text.readString(in));
        valuePattern = Pattern.compile(Text.readString(in));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TypeValueRegularExpressionPredicate predicate = (TypeValueRegularExpressionPredicate) o;

        if (typePattern != null ? !typePattern.pattern().equals(predicate.typePattern.pattern()) : predicate.typePattern != null)
            return false;
        if (valuePattern != null ? !valuePattern.pattern().equals(predicate.valuePattern.pattern()) : predicate.valuePattern != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = typePattern != null ? typePattern.hashCode() : 0;
        result = 31 * result + (valuePattern != null ? valuePattern.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TypeValueRegularExpressionPredicate{" +
                "typePattern=" + typePattern +
                ", valuePattern=" + valuePattern +
                '}';
    }
}
