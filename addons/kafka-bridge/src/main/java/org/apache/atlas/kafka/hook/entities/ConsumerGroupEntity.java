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

import java.util.HashMap;
import java.util.Map;

/**
 * A Kafka consumer group which is identified by its group ID.
 */
public class ConsumerGroupEntity extends AtlasKafkaEntity {

    private static final String KAFKA_CONSUMER_TYPENAME = "kafka_consumer_group";
    private static final String NAME = "name";
    private static final String GROUP_ID = "groupId";

    public ConsumerGroupEntity(String groupId) {
        super(KAFKA_CONSUMER_TYPENAME, groupId, LazyAtlasConfigStore.getInstance().getClientMetadataNamespace());
    }

    @Override
    protected Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NAME, getQualifiedName().getUniqueIdentifier());
        attributes.put(GROUP_ID, getQualifiedName().getUniqueIdentifier());
        return attributes;
    }
}
