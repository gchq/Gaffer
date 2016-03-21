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

package gaffer.tuple.view;

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A <code>Reference</code> refers to either a single field or contains a tuple of other <code>Reference</code>s.
 * It can be used to create a {@link gaffer.tuple.view.View} for a {@link gaffer.tuple.Tuple} to present it's flat data
 * structure in multi-dimensions.
 * @param <R> The type of reference used by tuples.
 */
public class Reference<R> {
    @JsonIgnore
    private Type referenceType;
    private R field;
    private Reference[] tuple;

    /**
     * Create a new <code>Reference</code>.
     */
    public Reference() { }

    /**
     * Create a new <code>Reference</code> with the given field references.
     * @param fields Field references.
     */
    public Reference(final R... fields) {
        if (fields.length == 1) {
            setField(fields[0]);
        } else {
            setFields(fields);
        }
    }

    /**
     * Set this <code>Reference</code> to refer to a single field.
     * @param field Single field reference.
     */
    public void setField(final R field) {
        referenceType = Type.FIELD;
        this.field = field;
        tuple = null;
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
     * Set this <code>Reference</code> to refer to a tuple of field references.
     * @param fields Field references.
     */
    public void setFields(final R... fields) {
        if (fields == null) {
            tuple = null;
        } else {
            Reference<R>[] references = new Reference[fields.length];
            int i = 0;
            for (R field : fields) {
                Reference<R> reference = new Reference<>(field);
                references[i++] = reference;
            }
            setTupleReferences(references);
        }
        field = null;
        referenceType = Type.FIELDS;
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
            for (Reference<R> reference : tuple) {
                fields[i++] = reference.getField();
            }
            return fields;
        } else {
            return null;
        }
    }

    /**
     * Set this <code>Reference</code> to refer to a tuple of <code>Reference</code>s.
     * @param tuple Tuple of references.
     */
    public void setTuple(final Reference... tuple) {
        if (tuple == null) {
            this.tuple = null;
        } else {
            setTupleReferences(tuple);
        }
        field = null;
        referenceType = Type.TUPLE;
    }

    /**
     * @return Tuple of references.
     */
    @SuppressFBWarnings(value = {"EI_EXPOSE_REP", "PZLA_PREFER_ZERO_LENGTH_ARRAYS"},
            justification = "This class is designed to simply wrap references - null is not set.")
    public Reference[] getTuple() {
        if (referenceType == Type.TUPLE) {
            return getTupleReferences();
        } else {
            return null;
        }
    }

    /**
     * Sets this <code>Reference</code> to refer to a tuple of references. If all of the supplied references are field
     * references, then the type will be set to <code>Type.FIELDS</code>, otherwise it will be <code>Type.TUPLE</code>.
     * @param references Tuple references.
     */
    public void setTupleReferences(final Reference... references) {
        boolean allFields = true;
        for (Reference reference : references) {
            allFields = allFields && reference.isFieldReference();
        }
        referenceType = allFields ? Type.FIELDS : Type.TUPLE;
        this.tuple = references;
    }

    /**
     * @return Tuple of references.
     */
    @SuppressFBWarnings(value = {"EI_EXPOSE_REP", "PZLA_PREFER_ZERO_LENGTH_ARRAYS"},
            justification = "This class is designed to simply wrap references - null is not set.")
    @JsonIgnore
    public Reference[] getTupleReferences() {
        if (isTupleReference()) {
            return tuple;
        } else {
            return null;
        }
    }

    /**
     * Create a new {@link gaffer.tuple.view.View} from this <code>Reference</code>.
     * @return View for tuples based on this reference.
     */
    public View<R> createView() {
        if (isFieldReference()) {
            return field == null ? null : new FieldView<>(this);
        } else if (isTupleReference()) {
            return tuple == null ? null : new TupleView<>(this);
        } else {
            return null;
        }
    }

    /**
     * @return True if this <code>Reference</code> contains a single field reference.
     */
    @JsonIgnore
    public boolean isFieldReference() {
        return referenceType == Type.FIELD;
    }

    /**
     * @return True if this <code>Reference</code> contains a tuple of field references.
     */
    @JsonIgnore
    public boolean isTupleReference() {
        return referenceType == Type.FIELDS || referenceType == Type.TUPLE;
    }

    /**
     * Denotes the type of reference (either FIELD, FIELDS or TUPLE).
     */
    private enum Type {
        FIELD, FIELDS, TUPLE;
    }
}
