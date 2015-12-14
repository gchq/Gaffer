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
package gaffer.accumulo.predicate.impl;

import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.graph.Entity;
import gaffer.predicate.Predicate;
import gaffer.predicate.typevalue.TypeValuePredicate;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link Predicate} of {@link RawGraphElementWithStatistics} that returns true if the second
 * end of the edge matches the supplied {@link TypeValuePredicate}, or if it represents
 * an {@link Entity}. This is used to filter the other end of edges when querying, e.g.
 * it can be used to query for customer|A, but restrict the edges returned so that the
 * value at the other end matches *abc*.
 */
public class OtherEndOfEdgePredicate implements Predicate<RawGraphElementWithStatistics> {

    private static final long serialVersionUID = 730043645244297267L;
    private TypeValuePredicate predicate;

    public OtherEndOfEdgePredicate() { }

    public OtherEndOfEdgePredicate(TypeValuePredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public boolean accept(RawGraphElementWithStatistics rawGraphElementWithStatistics) throws IOException {
        if (rawGraphElementWithStatistics.isEntity()) {
            return true;
        }
        String secondType = rawGraphElementWithStatistics.getSecondType();
        String secondValue = rawGraphElementWithStatistics.getSecondValue();
        return predicate.accept(secondType, secondValue);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, predicate.getClass().getName());
        predicate.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String className = Text.readString(in);
        try {
            predicate = (TypeValuePredicate) Class.forName(className).newInstance();
            predicate.readFields(in);
        } catch (InstantiationException e) {
            throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
        } catch (IllegalAccessException e) {
            throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
        } catch (ClassNotFoundException e) {
            throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
        } catch (ClassCastException e) {
            throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OtherEndOfEdgePredicate that = (OtherEndOfEdgePredicate) o;

        if (predicate != null ? !predicate.equals(that.predicate) : that.predicate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return predicate != null ? predicate.hashCode() : 0;
    }
}
