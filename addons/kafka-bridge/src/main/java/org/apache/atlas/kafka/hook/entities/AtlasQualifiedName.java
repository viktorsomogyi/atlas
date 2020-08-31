/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.kafka.hook.entities;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.type.AtlasTypeUtil;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * <p>A representation of the Atlas qualified name to collect helper methods and encapsulate functionality.
 * </p>
 * <p>A qualified name is a unique name across Atlas that consists of two parts:
 * <ul>
 *     <li>A unique identifier that is unique to the given entity type, such as a topic name or a client ID</li>
 *     <li>A metadata namespace that identifies the service where the above unique identifier exists</li>
 * </ul>
 * </p>
 */
public class AtlasQualifiedName {

    public static final String ATTRIBUTE_QUALIFIED_NAME = AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME;

    private static final String SEPARATOR = "@";
    private static final String FORMAT_KAKFA_TOPIC_QUALIFIED_NAME = "%s" + SEPARATOR + "%s";
    private final String uniqueIdentifier;
    private final String metadataNamespace;
    private final String qualifiedName;

    /**
     * Creates a new qualified name object that consists of a unique identifier that is unique to the type of the name,
     * such as the name of the topic and the metadata namespace. This together forms a "topicName@metadataNamespace"
     * string which is called the qualified name.
     * @param uniqueIdentifier
     * @param metadataNamespace
     */
    private AtlasQualifiedName(String uniqueIdentifier, String metadataNamespace) {
        this.uniqueIdentifier = uniqueIdentifier;
        this.metadataNamespace = metadataNamespace;
        this.qualifiedName = String.format(FORMAT_KAKFA_TOPIC_QUALIFIED_NAME, uniqueIdentifier, metadataNamespace);
    }

    public String getUniqueIdentifier() {
        return uniqueIdentifier;
    }

    public String getMetadataNamespace() {
        return metadataNamespace;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public void addNameToAttributeMap(Map<String, ? super String> attributeMap) {
        attributeMap.put(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
    }

    public Map<String, String> getAsAttributeMap() {
        return Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
    }

    public static Optional<AtlasQualifiedName> of(String qualifiedName) {
        String[] parts = qualifiedName.split(SEPARATOR);
        if (parts.length != 2) {
            return Optional.empty();
        }
        return Optional.of(new AtlasQualifiedName(parts[0], parts[1]));
    }

    public static Optional<AtlasQualifiedName> of(AtlasEntityHeader header) {
        return of((String) header.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
    }

    public static AtlasQualifiedName of(String uniqueIdentifier, String metadataNamespace) {
        return new AtlasQualifiedName(uniqueIdentifier, metadataNamespace);
    }

    public static Optional<AtlasQualifiedName> of(AtlasEntity atlasEntity) {
        return AtlasQualifiedName.of((String) atlasEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasQualifiedName that = (AtlasQualifiedName) o;
        return qualifiedName.equals(that.qualifiedName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifiedName);
    }

    @Override
    public String toString() {
        return qualifiedName;
    }
}
