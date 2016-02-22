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
package gaffer.accumulostore.key.core.impl.model;

import org.apache.accumulo.core.data.Value;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ColumnQualifierColumnVisibilityValueTriple {
    byte[] columnQualifier;
    byte[] columnVisibility;
    Value value = new Value();

    public ColumnQualifierColumnVisibilityValueTriple() {
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "required for performance")
    public ColumnQualifierColumnVisibilityValueTriple(final byte[] columnQualifier, final byte[] columnVisibility,
            final Value value) {
        this.columnQualifier = columnQualifier;
        this.columnVisibility = columnVisibility;
        this.value = value;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "required for performance")
    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "required for performance")
    public void setColumnQualifier(final byte[] columnQualifier) {
        this.columnQualifier = columnQualifier;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "required for performance")
    public byte[] getColumnVisibility() {
        return columnVisibility;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "required for performance")
    public void setColumnVisibility(final byte[] columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public Value getValue() {
        return value;
    }

    public void setValue(final Value value) {
        this.value = value;
    }
}
