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
package gaffer.accumulo.predicate;

import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.Predicate;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link Predicate} of {@link RawGraphElementWithStatistics} that is constructed from a {@link Predicate}
 * of {@link GraphElementWithStatistics}. Given a {@link RawGraphElementWithStatistics}, it can simply call
 * {@link #accept} on the provided {@link GraphElementWithStatistics} to decide if this is wanted, as a
 * {@link RawGraphElementWithStatistics} is a subclass of {@link RawGraphElementWithStatistics}.
 */
public class RawGraphElementWithStatisticsPredicate implements Predicate<RawGraphElementWithStatistics> {

    private static final long serialVersionUID = -2230592960941043901L;
    private Predicate<GraphElementWithStatistics> predicate;

    public RawGraphElementWithStatisticsPredicate() { }

    public RawGraphElementWithStatisticsPredicate(Predicate<GraphElementWithStatistics> predicate) {
        this.predicate = predicate;
    }

    @Override
    public boolean accept(RawGraphElementWithStatistics rawGraphElementWithStatistics) throws IOException {
        return predicate.accept(rawGraphElementWithStatistics);
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
            predicate = (Predicate<GraphElementWithStatistics>) Class.forName(className).newInstance();
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
    public String toString() {
        return "RawGraphElementWithStatisticsPredicate{" +
                "predicate=" + predicate +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RawGraphElementWithStatisticsPredicate predicate1 = (RawGraphElementWithStatisticsPredicate) o;

        if (predicate != null ? !predicate.equals(predicate1.predicate) : predicate1.predicate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return predicate != null ? predicate.hashCode() : 0;
    }
}
