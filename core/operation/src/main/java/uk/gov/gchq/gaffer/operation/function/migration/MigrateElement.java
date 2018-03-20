/* Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.function.migration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.List;

@JsonPropertyOrder(value = {"oldGroup", "newGroup", "toNewFunctions", "toOldFunctions"}, alphabetic = true)
public class MigrateElement {
    private String oldGroup;
    private String newGroup;
    private ElementTransformer toNewTransform;
    private ElementTransformer toOldTransform;

    public MigrateElement() {
    }

    public void setOldGroup(String oldGroup) {
        this.oldGroup = oldGroup;
    }

    public String getOldGroup() {
        return oldGroup;
    }

    public void setNewGroup(String newGroup) {
        this.newGroup = newGroup;
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
        if (null != toNewFunctions) {
            this.toNewTransform.getComponents().addAll(toNewFunctions);
        }
    }

    public List<TupleAdaptedFunction<String, ?, ?>> getToOld() {
        return null != toOldTransform ? toOldTransform.getComponents() : null;
    }

    public void setToOld(final List<TupleAdaptedFunction<String, ?, ?>> toOldFunctions) {
        this.toOldTransform = new ElementTransformer();
        if (null != toOldFunctions) {
            this.toOldTransform.getComponents().addAll(toOldFunctions);
        }
    }
}
