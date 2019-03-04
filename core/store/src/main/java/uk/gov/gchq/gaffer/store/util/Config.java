/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.store.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Config {
    private String id;
    private String description;
    private List<Hook> hooks = new ArrayList<>();

    public Config() {
    }

    public Config(final String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }


    public List<Hook> getHooks() {
        return hooks;
    }

    public void setHooks(final List<Hook> hooks) {
        if (null == hooks) {
            this.hooks.clear();
        } else {
            hooks.forEach(this::addHook);
        }
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }


    public void addHook(final Hook hook) {
        if (null != hook) {
            if (hook instanceof HookPath) {
                final String path = ((HookPath) hook).getPath();
                final File file = new File(path);
                if (!file.exists()) {
                    throw new IllegalArgumentException("Unable to find graph hook file: " + path);
                }
                try {
                    hooks.add(JSONSerialiser.deserialise(FileUtils.readFileToByteArray(file), Hook.class));
                } catch (final IOException e) {
                    throw new IllegalArgumentException("Unable to deserialise graph hook from file: " + path, e);
                }
            } else {
                hooks.add(hook);
            }
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("hooks", hooks)
                .toString();
    }

    public static class Builder {
        private Config config = new Config();

        public Builder json(final Path path) {
            try {
                return json(null != path ? Files.readAllBytes(path) : null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from path: " + path, e);
            }
        }

        public Builder json(final URI uri) {
            try {
                json(null != uri ? StreamUtil.openStream(uri) : null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from uri: " + uri, e);
            }

            return this;
        }

        public Builder json(final InputStream stream) {
            try {
                json(null != stream ? IOUtils.toByteArray(stream) : null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from input stream", e);
            }

            return this;
        }

        public Builder json(final byte[] bytes) {
            if (null != bytes) {
                try {
                    merge(JSONSerialiser.deserialise(bytes, Config.class));
                } catch (final IOException e) {
                    throw new IllegalArgumentException("Unable to deserialise graph config", e);
                }
            }
            return this;
        }

        public Builder merge(final Config config) {
            if (null != config) {
                if (null != config.getId()) {
                    this.config.setId(config.getId());
                }
                if (null != config.getDescription()) {
                    this.config.setDescription(config.getDescription());
                }
                this.config.getHooks().addAll(config.getHooks());
            }
            return this;
        }

        public Builder id(final String id) {
            config.setId(id);
            return this;
        }

        public Builder description(final String description) {
            this.config.setDescription(description);
            return this;
        }

        public Builder addHooks(final Path hooksPath) {
            if (null == hooksPath || !hooksPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hooks file: " + hooksPath);
            }
            final Hook[] hooks;
            try {
                hooks =
                        JSONSerialiser.deserialise(FileUtils.readFileToByteArray(hooksPath.toFile()), Hook[].class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hooks file: " + hooksPath, e);
            }
            return addHooks(hooks);
        }

        public Builder addHook(final Path hookPath) {
            if (null == hookPath || !hookPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hook file: " + hookPath);
            }

            final Hook hook;
            try {
                hook =
                        JSONSerialiser.deserialise(FileUtils.readFileToByteArray(hookPath.toFile()), Hook.class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hook file: " + hookPath, e);
            }
            return addHook(hook);
        }

        public Builder addHook(final Hook hook) {
            if (null != hook) {
                this.config.addHook(hook);
            }
            return this;
        }

        public Builder addHooks(final Hook... hooks) {
            if (null != hooks) {
                Collections.addAll(this.config.getHooks(), hooks);
            }
            return this;
        }

        public Config build() {
            return config;
        }

        @Override
        public String toString() {
            return config.toString();
        }
    }
}
