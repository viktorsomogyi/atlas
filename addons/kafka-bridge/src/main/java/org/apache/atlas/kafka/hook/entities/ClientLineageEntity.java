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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClientLineageEntity extends AtlasKafkaEntity {

    private static final String NAME = "name";
    private static final String LINEAGE_POSTFIX = "lineage";

    public ClientLineageEntity(String typeName, String lineageName) {
        super(typeName, lineageName, LazyAtlasConfigStore.getInstance().getClientMetadataNamespace());
    }

    public static ClientLineageEntity of(AtlasKafkaClientEntity client, List<TopicEntity> topics) {
        String lineageName = client.toAtlasEntity().getAttribute(NAME).toString() + "_" + LINEAGE_POSTFIX;
        return client.createClientLineageEntity(lineageName, topics.stream().map(AtlasKafkaEntity::toAtlasEntity).collect(Collectors.toList()));
    }

    @Override
    protected Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(NAME, getQualifiedName().getUniqueIdentifier());
        return attributes;
    }
}
