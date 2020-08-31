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

import java.util.Map;
import java.util.Objects;

/**
 * Represents a Kafka related entity in Atlas.
 */
public abstract class AtlasKafkaEntity {

    private static final String CLUSTERNAME = "clusterName";

    private final String typeName;
    private final AtlasQualifiedName qualifiedName;

    protected AtlasEntity entity;

    /**
     * Creates a new entity.
     * @param typeName is an Atlas specific name of the typedef that the created object implements.
     * @param uniqueIdentifier is a unique name, such as topic name, client id, that identifies an entity within the
     *                         metadata namespace.
     * @param metadataNamespace is an Atlas specific name which represents the cluster or other logical grouping that
     *                          this entity belongs to.
     */
    public AtlasKafkaEntity(String typeName, String uniqueIdentifier, String metadataNamespace) {
        this.typeName = typeName;
        this.qualifiedName = AtlasQualifiedName.of(uniqueIdentifier, metadataNamespace);
    }

    public AtlasKafkaEntity(String typeName, AtlasQualifiedName qualifiedName) {
        this.typeName = typeName;
        this.qualifiedName = qualifiedName;
    }

    public AtlasEntity toAtlasEntity() {
        if(entity == null) {
            entity = new AtlasEntity(typeName);

            Map<String, Object> attributes = getAttributes();
            entity.setAttributes(attributes);
            qualifiedName.addNameToAttributeMap(attributes);
            entity.setAttribute(CLUSTERNAME, getMetadataNamespace());
            entity.setGuid(null);
        }
        return entity;
    }

    public AtlasQualifiedName getQualifiedName() {
        return qualifiedName;
    }

    public String getMetadataNamespace() {
        return qualifiedName.getMetadataNamespace();
    }

    protected abstract Map<String, Object> getAttributes();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasKafkaEntity that = (AtlasKafkaEntity) o;
        return typeName.equals(that.typeName) &&
                qualifiedName.equals(that.qualifiedName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, qualifiedName);
    }

    @Override
    public String toString() {
        return "AtlasKafkaEntity{" +
                "metadataNamespace='" + getMetadataNamespace() + '\'' +
                ", uniqueIdentifier='" + qualifiedName.getUniqueIdentifier() + '\'' +
                ", typeName='" + typeName + '\'' +
                '}';
    }
}
