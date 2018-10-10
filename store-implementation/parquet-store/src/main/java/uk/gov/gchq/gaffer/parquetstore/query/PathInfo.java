/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

public class PathInfo {

    public enum FILETYPE {
        ENTITY, EDGE, REVERSED_EDGE
    }

    private final Path path;
    private final String group;
    private final FILETYPE fileType;

    public PathInfo(final Path path, final String group, final FILETYPE fileType) {
        this.path = path;
        this.group = group;
        this.fileType = fileType;
    }

    public Path getPath() {
        return path;
    }

    public String getGroup() {
        return group;
    }

    public FILETYPE getFileType() {
        return fileType;
    }

    public boolean isReversed() {
        return fileType == FILETYPE.REVERSED_EDGE;
    }

    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof PathInfo)
                && equals((PathInfo) obj);
    }

    public boolean equals(final PathInfo pathInfo) {
        return null != pathInfo
                && new EqualsBuilder()
                .append(path, pathInfo.path)
                .append(group, pathInfo.group)
                .append(fileType, pathInfo.fileType)
                .isEquals();
    }

    public int hashCode() {
        return new HashCodeBuilder(23, 5)
                .append(path)
                .append(group)
                .append(fileType)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("path", path)
                .append("group", group)
                .append("fileType", fileType)
                .build();
    }
}
