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

import com.beust.jcommander.IStringConverter;

import uk.gov.gchq.gaffer.user.User;

public class UserConverter implements IStringConverter<User> {
    private final ArgumentConverter argumentConverter;

    public UserConverter() {
        this.argumentConverter = new ArgumentConverter();
    }

    UserConverter(final ArgumentConverter argumentConverter) {
        this.argumentConverter = argumentConverter;
    }

    @Override
    public User convert(final String value) {
        return argumentConverter.convert(value, User.class);
    }
}
