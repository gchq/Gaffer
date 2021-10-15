/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.factory;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnknownUserFactoryTest {

    @Test
    public void shouldCreateUserWithUnknownId() {
        // When
        UserFactory userFactory = new UnknownUserFactory();

        // Then
        assertEquals("UNKNOWN", userFactory.createUser().getUserId());
    }

    @Test
    public void shouldAddUserOpAuth() {
        // When
        UserFactory userFactory = new UnknownUserFactory();

        // Then
        assertEquals(Sets.newHashSet("user"), userFactory.createUser().getOpAuths());
    }
}
