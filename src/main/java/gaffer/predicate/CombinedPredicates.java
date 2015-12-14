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
package gaffer.predicate;

import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Creates a {@link Predicate} by combining two existing {@link Predicate}s. They can be
 * combined either by ANDing, ORing or XORing.
 *
 * @param <T>
 */
public class CombinedPredicates<T> implements Predicate<T> {

    private static final long serialVersionUID = -3864823351685945201L;

    public enum Combine {AND, OR, XOR}

    private Predicate<T> predicate1;
    private Predicate<T> predicate2;
    private Combine combine;

    public CombinedPredicates() { }

    public CombinedPredicates(Predicate<T> predicate1,
                              Predicate<T> predicate2, Combine combine) {
        this.predicate1 = predicate1;
        this.predicate2 = predicate2;
        this.combine = combine;
    }

    @Override
    public boolean accept(T t) throws IOException {
        switch (combine) {
            case AND:
                return predicate1.accept(t) && predicate2.accept(t);
            case OR:
                return predicate1.accept(t) || predicate2.accept(t);
            case XOR:
                return predicate1.accept(t) ^ predicate2.accept(t);
            default:
                return false;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, predicate1.getClass().getName());
        predicate1.write(out);
        Text.writeString(out, predicate2.getClass().getName());
        predicate2.write(out);
        out.writeInt(combine.ordinal());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            String className1 = Text.readString(in);
            predicate1 = (Predicate<T>) Class.forName(className1).newInstance();
            predicate1.readFields(in);
            String className2 = Text.readString(in);
            predicate2 = (Predicate<T>) Class.forName(className2).newInstance();
            predicate2.readFields(in);
        } catch (InstantiationException e) {
            throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
        } catch (IllegalAccessException e) {
            throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
        } catch (ClassNotFoundException e) {
            throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
        } catch (ClassCastException e) {
            throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
        }
        combine = Combine.values()[in.readInt()];
    }

    @Override
    public String toString() {
        return "CombinedPredicates{" +
                "predicate1=" + predicate1 +
                ", predicate2=" + predicate2 +
                ", combine=" + combine +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CombinedPredicates that = (CombinedPredicates) o;

        if (combine != that.combine) return false;
        if (predicate1 != null ? !predicate1.equals(that.predicate1) : that.predicate1 != null) return false;
        if (predicate2 != null ? !predicate2.equals(that.predicate2) : that.predicate2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = predicate1 != null ? predicate1.hashCode() : 0;
        result = 31 * result + (predicate2 != null ? predicate2.hashCode() : 0);
        result = 31 * result + (combine != null ? combine.hashCode() : 0);
        return result;
    }
}
