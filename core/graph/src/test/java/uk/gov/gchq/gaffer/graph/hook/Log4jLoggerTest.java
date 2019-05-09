/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class Log4jLoggerTest extends GraphHookTest<Log4jLogger> {

    public Log4jLoggerTest() {
        super(Log4jLogger.class);
    }

    @Test
    public void shouldReturnResultWithoutModification() {
        // Given
        final Log4jLogger hook = getTestObject();
        final Object result = mock(Object.class);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GenerateObjects<>())
                .build();
        final User user = new User.Builder()
                .opAuths("NoScore")
                .build();

        // When
        final Object returnedResult = hook.postExecute(result, opChain, new Context(new User()));

        // Then
        assertSame(result, returnedResult);
    }

    @Override
    public Log4jLogger getTestObject() {
        return new Log4jLogger();
    }
}
