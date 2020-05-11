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
package uk.gov.gchq.gaffer.operation.runner.argument.converter;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserConverterTest {
    private static final String PATH_TO_SERIALISED_USER = "/path";
    private ArgumentConverter argumentConverter;
    private UserConverter userConverter;

    @Before
    public void createOperationChainConverter() {
        argumentConverter = mock(ArgumentConverter.class);
        userConverter = new UserConverter(argumentConverter);
    }

    @Test
    public void shouldConvertPathToSerialisedUserToUserInstance() {
        final User user = new User.Builder()
                .userId("userId")
                .dataAuth("dataAuth")
                .opAuth("opAuth")
                .build();
        when(argumentConverter.convert(PATH_TO_SERIALISED_USER, User.class)).thenReturn(user);
        final User resultUser = userConverter.convert(PATH_TO_SERIALISED_USER);
        assertEquals(user, resultUser);
        verify(argumentConverter).convert(PATH_TO_SERIALISED_USER, User.class);
    }
}
