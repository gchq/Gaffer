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
import org.apache.commons.collections.CollectionUtils;

import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer.Builder;
import uk.gov.gchq.gaffer.operation.function.migration.Identity;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.List;

@JsonPropertyOrder(value = {"oldGroup", "newGroup", "toNewFunctions", "toOldFunctions"}, alphabetic = true)
public class MigrateElement {
    private static ElementType ELEMENT_TYPE = null;
    private String oldGroup;
    private String newGroup;
    private ElementTransformer toNewTransform;
    private ElementTransformer toOldTransform;

    public enum ElementType {
        EDGE, ENTITY
    }

    public MigrateElement() {
    }

    public MigrateElement(final ElementType elementType, final String oldGroup, final String newGroup,
                          final ElementTransformer toNewTransform,
                          final ElementTransformer toOldTransform) {
        this.ELEMENT_TYPE = elementType;
        this.oldGroup = oldGroup;
        this.newGroup = newGroup;
        this.toNewTransform = toNewTransform;
        this.toOldTransform = toOldTransform;
        addNewGroupTransform();
        addOldGroupTransform();
    }

    public void setElementType(final ElementType elementType) {
        this.ELEMENT_TYPE = elementType;
    }

    public ElementType getElementType() {
        return ELEMENT_TYPE;
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
        return toNewTransform;
    }

    @JsonIgnore
    public ElementTransformer getToOldTransform() {
        return toOldTransform;
    }

    public List<TupleAdaptedFunction<String, ?, ?>> getToNew() {
        return null != toNewTransform ? toNewTransform.getComponents() : null;
    }

    public void setToNew(final List<TupleAdaptedFunction<String, ?, ?>> toNewFunctions) {
        this.toNewTransform = new ElementTransformer();
        addNewGroupTransform();
        if (null != toNewFunctions) {
            this.toNewTransform.getComponents().addAll(toNewFunctions);
        }
    }

    public List<TupleAdaptedFunction<String, ?, ?>> getToOld() {
        return null != toOldTransform ? toOldTransform.getComponents() : null;
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
            final ElementTransformer toNewTransformTmp = new Builder()
                    .select("GROUP")
                    .execute(new Identity(newGroup))
                    .project("GROUP")
                    .build();
            if (null != toNewTransform && CollectionUtils.isNotEmpty(toNewTransform.getComponents())) {
                toNewTransformTmp.getComponents().addAll(toNewTransform.getComponents());
            }
            toNewTransform = toNewTransformTmp;
        }
    }

    private void addOldGroupTransform() {
        if (null != oldGroup && !oldGroup.equals(newGroup)) {
            final ElementTransformer toOldTransformTmp = new Builder()
                    .select("GROUP")
                    .execute(new Identity(oldGroup))
                    .project("GROUP")
                    .build();
            if (null != toOldTransform && CollectionUtils.isNotEmpty(toOldTransform.getComponents())) {
                toOldTransformTmp.getComponents().addAll(toOldTransform.getComponents());
            }
            toOldTransform = toOldTransformTmp;
        }
    }
}
