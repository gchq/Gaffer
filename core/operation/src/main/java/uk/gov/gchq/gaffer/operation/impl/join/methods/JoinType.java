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

package uk.gov.gchq.gaffer.operation.impl.join.methods;

public enum JoinType {
    FULL(FullJoin.class),
    FULL_INNER(FullInnerJoin.class),
    FULL_OUTER(FullOuterJoin.class),
    OUTER(OuterJoin.class),
    INNER(InnerJoin.class);

    private final Class<? extends JoinFunction> className;

    JoinType(final Class<? extends JoinFunction> className) {
        this.className = className;
    }

    public JoinFunction createInstance() {
        try {
            return className.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
