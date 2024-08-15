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

package uk.gov.gchq.gaffer.jsonserialisation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import uk.gov.gchq.gaffer.exception.SerialisationException;

import java.io.File;
import java.io.IOException;

public class YAMLDeserialiser {

  private static YAMLDeserialiser instance;
  private ObjectMapper objectMapper;

  protected YAMLDeserialiser() {
    objectMapper = new ObjectMapper(new YAMLFactory());
    objectMapper.findAndRegisterModules();
  }

  public static <T> T deserialise(final File file, final Class<T> clazz) throws SerialisationException {
    try {
      return getInstance().objectMapper.readValue(file, clazz);
    } catch (final IOException e) {
      throw new SerialisationException(e.getMessage(), e);
    }
  }

  public static YAMLDeserialiser getInstance() {
    if (instance == null) {
      instance = new YAMLDeserialiser();
    }

    return instance;
  }
}
