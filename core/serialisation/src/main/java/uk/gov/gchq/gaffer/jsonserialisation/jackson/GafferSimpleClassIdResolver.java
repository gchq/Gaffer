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

package uk.gov.gchq.gaffer.jsonserialisation.jackson;

import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;

import uk.gov.gchq.koryphe.impl.function.CreateObject;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

public class GafferSimpleClassIdResolver extends SimpleClassNameIdResolver {

    private static final PolymorphicTypeValidator TYPE_VALIDATOR;

    static {
        TYPE_VALIDATOR = BasicPolymorphicTypeValidator.builder()
            .allowIfSubType(new BasicPolymorphicTypeValidator.TypeMatcher() {
                @Override
                public boolean match(final MapperConfig<?> mapperConfig, final Class<?> aClass) {
                    return !CreateObject.class.equals(aClass);
                }
            })
            .build();
    }

    @Override
    protected PolymorphicTypeValidator createDefaultTypeValidator() {
        return TYPE_VALIDATOR;
    }
}
