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

import gaffer.Pair;
import gaffer.predicate.typevalue.TypeValuePredicate;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link TypeValuePredicate} that indicates whether the (type, value) is in the provided set.
 */
public class TypeValueInSetPredicate implements TypeValuePredicate {

    private static final long serialVersionUID = 3219899722822794189L;
    private Set<Pair<String>> allowedTypeValues;

    public TypeValueInSetPredicate() {
        this.allowedTypeValues = new HashSet<Pair<String>>();
    }

    public TypeValueInSetPredicate(Set<Pair<String>> allowedTypeValues) {
        this();
        this.allowedTypeValues.addAll(allowedTypeValues);
    }

    public TypeValueInSetPredicate(Pair<String>... allowedTypeValues) {
        this();
        Collections.addAll(this.allowedTypeValues, allowedTypeValues);
    }

    public void addAllowedTypeValues(Set<Pair<String>> allowedTypeValues) {
        this.allowedTypeValues.addAll(allowedTypeValues);
    }

    public void addAllowedTypeValues(Pair<String>... allowedTypeValues) {
        Collections.addAll(this.allowedTypeValues, allowedTypeValues);
    }

    public boolean accept(String type, String value) {
        return allowedTypeValues.contains(new Pair<String>(type, value));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        allowedTypeValues = new HashSet<Pair<String>>(size);
        for (int i = 0; i < size; i++) {
            allowedTypeValues.add(new Pair<String>(Text.readString(in), Text.readString(in)));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(allowedTypeValues.size());
        for (Pair<String> pair : allowedTypeValues) {
            Text.writeString(out, pair.getFirst());
            Text.writeString(out, pair.getSecond());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TypeValueInSetPredicate that = (TypeValueInSetPredicate) o;

        if (allowedTypeValues != null ? !allowedTypeValues.equals(that.allowedTypeValues) : that.allowedTypeValues != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return allowedTypeValues != null ? allowedTypeValues.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "TypeValueInSetPredicate{" +
                "allowedTypeValues=" + allowedTypeValues +
                '}';
    }
}
