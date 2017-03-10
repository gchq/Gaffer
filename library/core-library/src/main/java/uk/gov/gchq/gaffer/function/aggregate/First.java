/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.function.aggregate;

import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;
import java.util.function.BinaryOperator;

/**
 * An <code>First</code> is a {@link BinaryOperator} that assumes the
 * value will never change and just returns the first non null value it gets.
 */
public class First extends KorypheBinaryOperator<Object> {
    @Override
    protected Object _apply(final Object a, final Object b) {
        return a;
    }
}
