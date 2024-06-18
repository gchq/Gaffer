/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.util;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

import static org.assertj.core.api.Assertions.assertThat;

public class TypeSubTypeValueFactoryTest {

  @Test
  void shouldParseStringAsTstv() {
    String tstv = "t:one|st:two|v:three";
    Object result = TypeSubTypeValueFactory.parseStringAsTstvIfValid(tstv);
    assertThat(result)
      .isInstanceOf(TypeSubTypeValue.class)
      .extracting(r -> (TypeSubTypeValue) r)
      .isEqualTo(new TypeSubTypeValue("one", "two", "three"));
  }

  @Test
  void shouldParseComplexStringAsTstv() {
    String tstv = "t:one|one|st:two|two|v:three|three";
    Object result = TypeSubTypeValueFactory.parseStringAsTstvIfValid(tstv);
    assertThat(result)
      .isInstanceOf(TypeSubTypeValue.class)
      .extracting(r -> (TypeSubTypeValue) r)
      .isEqualTo(new TypeSubTypeValue("one|one", "two|two", "three|three"));
  }


  @Test
  void shouldNotParseStringAsTstv() {
    String notATstv = "not|a|tstv";
    Object result = TypeSubTypeValueFactory.parseStringAsTstvIfValid(notATstv);
    assertThat(result)
      .isInstanceOf(String.class)
      .extracting(r -> (String) r)
      .isEqualTo("not|a|tstv");
  }
}
