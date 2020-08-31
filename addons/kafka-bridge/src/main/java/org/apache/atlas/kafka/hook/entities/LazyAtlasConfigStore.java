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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;

public class LazyAtlasConfigStore {

    private static LazyAtlasConfigStore instance;

    public static LazyAtlasConfigStore getInstance() {
        if (instance == null) {
            synchronized (LazyAtlasConfigStore.class) {
                if (instance == null) {
                    instance = new LazyAtlasConfigStore();
                }
            }
        }
        return instance;
    }

    private static final String KAFKA_TOPIC_METADATA_NAMESPACE = "atlas.metadata.namespace.topic";
    private static final String KAFKA_CLIENT_METADATA_NAMESPACE = "atlas.metadata.namespace.client";
    private static final String ATLAS_METADATA_NAMESPACE = "atlas.metadata.namespace";
    private static final String CLUSTER_NAME_KEY = "atlas.cluster.name";
    private static final String DEFAULT_CLUSTER_NAME = "cm";

    private final Configuration configuration;

    private LazyAtlasConfigStore() {
        try {
            configuration = ApplicationProperties.get();
        } catch (AtlasException e) {
            throw new RuntimeException();
        }
    }

    public String getTopicMetadataNamespace() {
        String topicNamespace = configuration.getString(KAFKA_TOPIC_METADATA_NAMESPACE);
        if (topicNamespace == null || topicNamespace.isEmpty()) {
            return getAtlasMetadataNamespace();
        }
        return topicNamespace;
    }

    public String getClientMetadataNamespace() {
        String clientNamespace = configuration.getString(KAFKA_CLIENT_METADATA_NAMESPACE);
        if (clientNamespace == null ||clientNamespace.isEmpty()) {
            return getAtlasMetadataNamespace();
        }
        return clientNamespace;
    }

    public String getAtlasMetadataNamespace() {
        return configuration.getString(ATLAS_METADATA_NAMESPACE, getClusterName());
    }

    public String getClusterName() {
        return configuration.getString(CLUSTER_NAME_KEY, DEFAULT_CLUSTER_NAME);
    }
}
