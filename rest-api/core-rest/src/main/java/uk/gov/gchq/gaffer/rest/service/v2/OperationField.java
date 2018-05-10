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

/**
 * POJO to store details for a single user defined field in an {@link uk.gov.gchq.gaffer.operation.Operation}.
 */
public class OperationField {
    private String name;
    private String className;
    private boolean required;

    public OperationField() {

    }

    public OperationField(final String name, final boolean required, final String className) {
        this.name = name;
        this.required = required;
        this.className = className;
    }

    public String getName() {
        return name;
    }

    public boolean isRequired() {
        return required;
    }

    public String getClassName() {
        return className;
    }
}
