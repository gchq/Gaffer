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
 * A {@link TypeValuePredicate} that indicates whether the type is in the provided set.
 */
public class TypeInSetPredicate implements TypeValuePredicate {

    private static final long serialVersionUID = 2109899722822794189L;
    private Set<String> allowedTypes;

    public TypeInSetPredicate() {
        this.allowedTypes = new HashSet<String>();
    }

    public TypeInSetPredicate(Set<String> allowedTypes) {
        this();
        this.allowedTypes.addAll(allowedTypes);
    }

    public TypeInSetPredicate(String... allowedTypes) {
        this();
        Collections.addAll(this.allowedTypes, allowedTypes);
    }

    public void addAllowedTypes(Set<String> allowedTypes) {
        this.allowedTypes.addAll(allowedTypes);
    }

    public void addAllowedTypes(String... allowedTypes) {
        Collections.addAll(this.allowedTypes, allowedTypes);
    }

    public boolean accept(String type, String value) {
        return allowedTypes.contains(type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        allowedTypes = new HashSet<String>(size);
        for (int i = 0; i < size; i++) {
            allowedTypes.add(Text.readString(in));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(allowedTypes.size());
        for (String s : allowedTypes) {
            Text.writeString(out, s);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((allowedTypes == null) ? 0 : allowedTypes
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
        TypeInSetPredicate other = (TypeInSetPredicate) obj;
        if (allowedTypes == null) {
            if (other.allowedTypes != null)
                return false;
        } else if (!allowedTypes.equals(other.allowedTypes))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TypeInSetPredicate [allowedTypes=" + allowedTypes + "]";
    }
}
