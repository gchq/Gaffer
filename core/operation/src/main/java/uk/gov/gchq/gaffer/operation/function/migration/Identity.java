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

package uk.gov.gchq.gaffer.operation.function.migration;

import uk.gov.gchq.koryphe.function.KorypheFunction;

public class Identity extends KorypheFunction<Object, Object> {
    private Object identity;

    public Identity() {
    }

    public Identity(final Object returnValue) {
        this.identity = returnValue;
    }

    @Override
    public Object apply(final Object o) {
        return identity;
    }

    public Object getIdentity() {
        return identity;
    }

    public void setIdentity(final Object identity) {
        this.identity = identity;
    }
}
