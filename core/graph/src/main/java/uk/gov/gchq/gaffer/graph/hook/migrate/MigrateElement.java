/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook.migrate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer.Builder;
import uk.gov.gchq.koryphe.impl.function.SetValue;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.List;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

@JsonPropertyOrder(value = {"oldGroup", "newGroup", "toNewFunctions", "toOldFunctions"}, alphabetic = true)
public class MigrateElement {
    private ElementType elementType;
    private String oldGroup;
    private String newGroup;
    private ElementTransformer toNewTransform;
    private ElementTransformer toNewPrivateTransform;
    private ElementTransformer toOldTransform;
    private ElementTransformer toOldPrivateTransform;

    public enum ElementType {
        EDGE, ENTITY
    }

    public MigrateElement() {
    }

    public MigrateElement(final ElementType elementType, final String oldGroup, final String newGroup,
                          final ElementTransformer toNewTransform,
                          final ElementTransformer toOldTransform) {
        this.elementType = elementType;
        this.newGroup = newGroup;
        this.toNewTransform = toNewTransform;
        this.toOldTransform = toOldTransform;
        setOldGroup(oldGroup);
        setNewGroup(newGroup);
    }

    public void setElementType(final ElementType elementType) {
        this.elementType = elementType;
    }

    public ElementType getElementType() {
        return elementType;
    }

    public void setOldGroup(final String oldGroup) {
        this.oldGroup = oldGroup;
        addOldGroupTransform();
    }

    public String getOldGroup() {
        return oldGroup;
    }

    public void setNewGroup(final String newGroup) {
        this.newGroup = newGroup;
        addNewGroupTransform();
    }

    public String getNewGroup() {
        return newGroup;
    }

    @JsonIgnore
    public ElementTransformer getToNewTransform() {
        final ElementTransformer tmp = new ElementTransformer();
        if (null != toNewPrivateTransform && isNotEmpty(toNewPrivateTransform.getComponents())) {
            tmp.getComponents().addAll(toNewPrivateTransform.getComponents());
        }
        if (null != toNewTransform && isNotEmpty(toNewTransform.getComponents())) {
            tmp.getComponents().addAll(toNewTransform.getComponents());
        }
        return tmp;
    }

    @JsonIgnore
    public ElementTransformer getToOldTransform() {
        final ElementTransformer tmp = new ElementTransformer();
        if (null != toOldPrivateTransform && isNotEmpty(toOldPrivateTransform.getComponents())) {
            tmp.getComponents().addAll(toOldPrivateTransform.getComponents());
        }
        if (null != toOldTransform && isNotEmpty(toOldTransform.getComponents())) {
            tmp.getComponents().addAll(toOldTransform.getComponents());
        }
        return tmp;
    }

    public List<TupleAdaptedFunction<String, ?, ?>> getToNew() {
        return toNewTransform.getComponents();
    }

    public void setToNew(final List<TupleAdaptedFunction<String, ?, ?>> toNewFunctions) {
        this.toNewTransform = new ElementTransformer();
        addNewGroupTransform();
        if (null != toNewFunctions) {
            this.toNewTransform.getComponents().addAll(toNewFunctions);
        }
    }

    public List<TupleAdaptedFunction<String, ?, ?>> getToOld() {
        return toOldTransform.getComponents();
    }

    public void setToOld(final List<TupleAdaptedFunction<String, ?, ?>> toOldFunctions) {
        this.toOldTransform = new ElementTransformer();
        addOldGroupTransform();
        if (null != toOldFunctions) {
            this.toOldTransform.getComponents().addAll(toOldFunctions);
        }
    }

    private void addNewGroupTransform() {
        if (null != newGroup && !newGroup.equals(oldGroup)) {
            toNewPrivateTransform = new Builder()
                    .select("GROUP")
                    .execute(new SetValue(newGroup))
                    .project("GROUP")
                    .build();
        }
    }

    private void addOldGroupTransform() {
        if (null != oldGroup && !oldGroup.equals(newGroup)) {
            toOldPrivateTransform = new Builder()
                    .select("GROUP")
                    .execute(new SetValue(oldGroup))
                    .project("GROUP")
                    .build();
        }
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final MigrateElement migrateElement = (MigrateElement) obj;

        return new EqualsBuilder()
                .append(elementType, migrateElement.getElementType())
                .append(oldGroup, migrateElement.getOldGroup())
                .append(newGroup, migrateElement.getNewGroup())
                .append(toNewTransform, migrateElement.getToNewTransform())
                .append(toOldTransform, migrateElement.getToOldTransform())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(19, 39)
                .append(elementType)
                .append(oldGroup)
                .append(newGroup)
                .append(toNewTransform)
                .append(toOldTransform)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("elementType", elementType)
                .append("oldGroup", oldGroup)
                .append("newGroup", newGroup)
                .append("toNewTransform", toNewTransform)
                .append("toOldTransform", toOldTransform)
                .build();
    }
}
