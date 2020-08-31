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

import java.util.List;

/**
 * This class represents an entity such as the producer or the consumer that may have a topic relationship.
 */
public abstract class AtlasKafkaClientEntity extends AtlasKafkaEntity {
    protected static final String ATTRIBUTE_OUTPUTS = "outputs";
    protected static final String ATTRIBUTE_INPUTS = "inputs";
    protected static final String RELATIONSHIP_DATASET_PROCESS_INPUTS = "dataset_process_inputs";
    protected static final String RELATIONSHIP_PROCESS_DATASET_OUTPUTS = "process_dataset_outputs";

    public AtlasKafkaClientEntity(String atlasTypeName, String uniqueIdentifier) {
        super(atlasTypeName, uniqueIdentifier, LazyAtlasConfigStore.getInstance().getClientMetadataNamespace());
    }

    public abstract ClientLineageEntity createClientLineageEntity(String lineageName, List<AtlasEntity> topicEntities);
}
