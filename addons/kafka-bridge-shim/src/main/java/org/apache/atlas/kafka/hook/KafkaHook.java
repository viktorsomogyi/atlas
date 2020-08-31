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

package org.apache.atlas.kafka.hook;

import org.apache.atlas.plugin.classloader.AtlasPluginClassLoader;
import org.apache.kafka.server.auditor.AuditEvent;
import org.apache.kafka.server.auditor.Auditor;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaHook implements Auditor {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaHook.class);

    private static final String SERVICE_NAME = "kafka";
    private static final String ATLAS_PLUGIN_IMPL_CLASS = "org.apache.atlas.kafka.hook.KafkaHookImpl";

    private AtlasPluginClassLoader atlasPluginClassLoader;
    private Auditor atlasPluginImpl;

    private void activatePluginClassLoader() {
        if (atlasPluginClassLoader != null) {
            atlasPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader() {
        if (atlasPluginClassLoader != null ) {
            atlasPluginClassLoader.deactivate();
        }
    }

    @Override
    public void audit(AuditEvent event, AuthorizableRequestContext requestContext) {
        try {
            activatePluginClassLoader();

            atlasPluginImpl.audit(event, requestContext);
        } finally {
            deactivatePluginClassLoader();
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.debug("==> POC Shim.initialize()");

        try {
            atlasPluginClassLoader = AtlasPluginClassLoader.getInstance(SERVICE_NAME, this.getClass());

            @SuppressWarnings("unchecked")
            Class<Auditor> cls = (Class<Auditor>) Class.forName(ATLAS_PLUGIN_IMPL_CLASS, true, atlasPluginClassLoader);

            activatePluginClassLoader();

            atlasPluginImpl = cls.newInstance();
            atlasPluginImpl.configure(configs);
        } catch (Throwable excp) {
            throw new RuntimeException("Error instantiating Atlas plugin implementation", excp);
        } finally {
            deactivatePluginClassLoader();
            LOG.debug("<== POC Shim.initialize()");
        }
    }

    @Override
    public void close() throws Exception {
        try {
            activatePluginClassLoader();

            atlasPluginImpl.close();
        } finally {
            deactivatePluginClassLoader();
        }
    }
}
