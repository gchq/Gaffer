/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.rest.SystemProperty;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UserFactoryTest {

    @BeforeEach
    @AfterEach
    public void clearUserFactoryClassSystemProperty() {
        System.clearProperty(SystemProperty.USER_FACTORY_CLASS);
    }

    @Test
    public void shouldCreateDefaultUserFactoryWhenNoSystemProperty() {
        // Given
        System.clearProperty(SystemProperty.USER_FACTORY_CLASS);

        // When
        final UserFactory userFactory = UserFactory.createUserFactory();

        // Then
        assertEquals(UnknownUserFactory.class, userFactory.getClass());
    }

    @Test
    public void shouldCreateUserFactoryFromSystemPropertyClassName() {
        // Given
        System.setProperty(SystemProperty.USER_FACTORY_CLASS, UserFactoryForTest.class.getName());

        // When
        final UserFactory userFactory = UserFactory.createUserFactory();

        // Then
        assertEquals(UserFactoryForTest.class, userFactory.getClass());
    }

    @Test
    public void shouldThrowExceptionFromInvalidSystemPropertyClassName() {
        // Given
        System.setProperty(SystemProperty.USER_FACTORY_CLASS, "InvalidClassName");

        // When
        assertThatIllegalArgumentException().isThrownBy(() -> UserFactory.createUserFactory());
    }
}
