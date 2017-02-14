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

package uk.gov.gchq.gaffer.rest.example;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ExampleDomainObject {
    private Object[] ids;
    private String type;

    public ExampleDomainObject() {
    }

    public ExampleDomainObject(final String type, final Object... ids) {
        this.ids = ids;
        this.type = type;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Just an example object")
    public Object[] getIds() {
        return ids;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "Just an example object")
    public void setIds(final Object[] ids) {
        this.ids = ids;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }
}
