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

package uk.gov.gchq.gaffer.rest.service.v2;

import uk.gov.gchq.gaffer.operation.Operation;

import java.util.List;
import java.util.Set;

/**
 * POJO to store details for a user specified {@link uk.gov.gchq.gaffer.operation.Operation}
 * class.
 */
public class OperationDetail {
    private String name;
    private List<OperationField> fields;
    private Set<Class<? extends Operation>> next;
    private Operation exampleJson;

    public OperationDetail() {
    }

    public OperationDetail(final String name, final List<OperationField> fields, final Set<Class<? extends Operation>> next, final Operation exampleJson){
        this.name = name;
        this.fields = fields;
        this.next = next;
        this.exampleJson = exampleJson;
    }

    public String getName() {
        return name;
    }

    public List<OperationField> getFields() {
        return fields;
    }

    public Set<Class<? extends Operation>> getNext() {
        return next;
    }

    public Operation getExampleJson() {
        return exampleJson;
    }
}