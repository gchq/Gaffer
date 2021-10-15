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

package uk.gov.gchq.gaffer.commonutil;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class GroupUtilTest {
    private static final String INVALID_STRING = "inv@l1dStr|ng&^";
    private static final String VALID_STRING = "vAl1d-Str|ng";

    @Test
    public void shouldThrowExceptionWithInvalidStringName() {
       assertThatIllegalArgumentException()
               .isThrownBy(() -> GroupUtil.validateName(INVALID_STRING))
               .withMessage("Group is invalid: inv@l1dStr|ng&^, it must match regex: [a-zA-Z0-9|-]*");
    }

    @Test
    public void shouldPassValidationWithValidStringName() {
        assertThatNoException().isThrownBy(() -> GroupUtil.validateName(VALID_STRING));
    }
}
