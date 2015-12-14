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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link TypeValuePredicate} that indicates whether the value is in the provided set.
 */
public class ValueInSetPredicate implements TypeValuePredicate {

    private static final long serialVersionUID = 4329899722822794189L;
    private Set<String> allowedValues;

    public ValueInSetPredicate() {
        this.allowedValues = new HashSet<String>();
    }

    public ValueInSetPredicate(Set<String> allowedValues) {
        this();
        this.allowedValues.addAll(allowedValues);
    }

    public ValueInSetPredicate(String... allowedValues) {
        this();
        Collections.addAll(this.allowedValues, allowedValues);
    }

    public void addAllowedValues(Set<String> allowedValues) {
        this.allowedValues.addAll(allowedValues);
    }

    public void addAllowedValues(String... allowedValues) {
        Collections.addAll(this.allowedValues, allowedValues);
    }

    public boolean accept(String type, String value) {
        return allowedValues.contains(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        allowedValues = new HashSet<String>(size);
        for (int i = 0; i < size; i++) {
            allowedValues.add(Text.readString(in));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(allowedValues.size());
        for (String s : allowedValues) {
            Text.writeString(out, s);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((allowedValues == null) ? 0 : allowedValues
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
        ValueInSetPredicate other = (ValueInSetPredicate) obj;
        if (allowedValues == null) {
            if (other.allowedValues != null)
                return false;
        } else if (!allowedValues.equals(other.allowedValues))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ValueInSetPredicate{" +
                "allowedValues=" + allowedValues +
                '}';
    }
}
