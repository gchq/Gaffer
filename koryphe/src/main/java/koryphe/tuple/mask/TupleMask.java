/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package koryphe.tuple.mask;

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import koryphe.tuple.ArrayTuple;
import koryphe.tuple.Tuple;

import java.util.Iterator;

/**
 * A <code>TupleMask</code> refers to either a single field or contains a tuple of other <code>TupleMask</code>s.
 * @param <R> The type of reference used by tuples.
 * @param <T> The adapted type.
 */
public class TupleMask<R, T> implements Tuple<Integer> {
    @JsonIgnore
    private Type referenceType;
    private R field;
    private TupleMask<R, ?>[] tuple;
    private Tuple<R> context;

    /**
     * Create a new <code>TupleMask</code>.
     */
    public TupleMask() { }

    public void setContext(final Tuple<R> context) {
        this.context = context;
    }

    /**
     * Create a new <code>TupleMask</code> with the given field references.
     * @param fields Field references.
     */
    public TupleMask(final R... fields) {
        if (fields.length == 1) {
            setField(fields[0]);
        } else {
            setFields(fields);
        }
    }

    public TupleMask(final TupleMask<R, ?>... tuple) {
        setTupleReferences(tuple);
    }

    /**
     * Set this <code>TupleMask</code> to refer to a single field.
     * @param field Single field reference.
     */
    public void setField(final R field) {
        if (field != null) {
            referenceType = Type.FIELD;
            this.field = field;
            tuple = null;
        }
    }

    /**
     * @return Single field reference.
     */
    public R getField() {
        if (isFieldReference()) {
            return field;
        } else {
            return null;
        }
    }

    /**
     * Set this <code>TupleMask</code> to refer to a tuple of field references.
     * @param fields Field references.
     */
    public void setFields(final R... fields) {
        if (fields != null) {
            TupleMask<R, ?>[] references = new TupleMask[fields.length];
            int i = 0;
            for (R field : fields) {
                TupleMask<R, ?> reference = new TupleMask<>(field);
                references[i++] = reference;
            }
            setTupleReferences(references);
            field = null;
            referenceType = Type.FIELDS;
        }
    }

    /**
     * @return Field references.
     */
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "Null return is used to signal not set.")
    public R[] getFields() {
        if (referenceType == Type.FIELDS && tuple != null) {
            // each reference should be a single reference...
            R[] fields = (R[]) new Object[tuple.length];
            int i = 0;
            for (TupleMask<R, ?> reference : tuple) {
                fields[i++] = reference.getField();
            }
            return fields;
        } else {
            return null;
        }
    }

    /**
     * Set this <code>TupleMask</code> to refer to a tuple of <code>TupleMask</code>s.
     * @param tuple Tuple of references.
     */
    public void setTuple(final TupleMask... tuple) {
        if (tuple != null) {
            setTupleReferences(tuple);
            field = null;
            referenceType = Type.TUPLE;
        }
    }

    /**
     * @return Tuple of references.
     */
    @SuppressFBWarnings(value = {"EI_EXPOSE_REP", "PZLA_PREFER_ZERO_LENGTH_ARRAYS"},
            justification = "This class is designed to simply wrap references - null is not set.")
    public TupleMask[] getTuple() {
        if (referenceType == Type.TUPLE) {
            return getTupleReferences();
        } else {
            return null;
        }
    }

    /**
     * Sets this <code>TupleMask</code> to refer to a tuple of references. If all of the supplied references are field
     * references, then the type will be set to <code>Type.FIELDS</code>, otherwise it will be <code>Type.TUPLE</code>.
     * @param references Tuple references.
     */
    public void setTupleReferences(final TupleMask... references) {
        if (references != null) {
            boolean allFields = true;
            for (TupleMask reference : references) {
                allFields = allFields && reference.isFieldReference();
            }
            referenceType = allFields ? Type.FIELDS : Type.TUPLE;
            this.tuple = references;
        }
    }

    /**
     * @return Tuple of references.
     */
    @SuppressFBWarnings(value = {"EI_EXPOSE_REP", "PZLA_PREFER_ZERO_LENGTH_ARRAYS"},
            justification = "This class is designed to simply wrap references - null is not set.")
    @JsonIgnore
    public TupleMask[] getTupleReferences() {
        if (isTupleReference()) {
            return tuple;
        } else {
            return null;
        }
    }

    /**
     * @return True if this <code>TupleMask</code> contains a single field reference.
     */
    @JsonIgnore
    public boolean isFieldReference() {
        return referenceType == Type.FIELD;
    }

    /**
     * @return True if this <code>TupleMask</code> contains a tuple of field references.
     */
    @JsonIgnore
    public boolean isTupleReference() {
        return referenceType == Type.FIELDS || referenceType == Type.TUPLE;
    }

    @Override
    public void put(final Integer reference, final Object value) {
        TupleMask<R, ?> tupleView = tuple[reference];
        tupleView.setContext(context);
        tupleView.project(value);
    }

    @Override
    public Object get(final Integer reference) {
        if (context == null) {
            return null;
        } else {
            return tuple[reference].select(context);
        }
    }

    @Override
    public Iterable<Object> values() {
        ArrayTuple selected = new ArrayTuple(tuple.length);
        for (int i = 0; i < tuple.length; i++) {
            selected.put(i, get(i));
        }
        return selected;
    }

    @Override
    public Iterator<Object> iterator() {
        return values().iterator();
    }

    public T select(final Tuple<R> tuple) {
        if (tuple == null) {
            return null;
        } else if (isFieldReference()) {
            return (T) tuple.get(field);
        } else {
            setContext(tuple);
            return (T) this;
        }
    }

    public Tuple<R> project(final Object output) {
        if (context != null) {
            if (isFieldReference()) {
                context.put(field, output);
            } else {
                int i = 0;
                for (Object obj : (Iterable) output) {
                    put(i++, obj);
                }
            }
        }
        return context;
    }

    /**
     * Denotes the type of reference (either FIELD, FIELDS or TUPLE).
     */
    private enum Type {
        FIELD, FIELDS, TUPLE;
    }
}
