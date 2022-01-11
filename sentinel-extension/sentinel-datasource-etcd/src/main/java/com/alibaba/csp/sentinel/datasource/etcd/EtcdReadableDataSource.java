/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.datasource.etcd;

import com.alibaba.csp.sentinel.datasource.AbstractReadableDataSource;
import com.alibaba.csp.sentinel.datasource.DataSourceHolder;
import com.alibaba.csp.sentinel.log.RecordLog;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.watch.WatchEvent;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A read-only {@code DataSource} with Etcd backend. When the data in Etcd backend has been modified,
 * Etcd will automatically push the new value so that the dynamic configuration can be real-time.
 *
 * @author lianglin
 * @since 1.7.0
 * @author Jiajiangnan
 */
public class EtcdReadableDataSource<T> extends AbstractReadableDataSource<String, T> {


    private final String key;
    private final Charset charset;
    private final boolean autoRefresh;
    private final DataSourceHolder dataSourceHolder;

    private Client client;
    private Watch.Watcher watcher;

    public EtcdReadableDataSource(String key, Charset charset, DataSourceHolder dataSourceHolder) {
        this(key, charset, dataSourceHolder, true);
    }

    /**
     * Create an etcd data-source. The connection configuration will be retrieved from {@link EtcdConfig}.
     *
     * @param key    config key
     * @param dataSourceHolder data dataSourceHolder
     * @param autoRefresh
     */
    public EtcdReadableDataSource(String key, Charset charset, DataSourceHolder dataSourceHolder, boolean autoRefresh) {
        super(dataSourceHolder);

        this.key = key;
        this.charset = charset;
        this.dataSourceHolder = dataSourceHolder;
        this.autoRefresh = autoRefresh;

        init();
    }

    private void init() {
        initClient();
        loadInitialConfig();
        if(autoRefresh) {
            initWatcher();
        }
    }

    private void initClient() {
        this.client = dataSourceHolder.getDataSourceClient() == null ? null : (Client) dataSourceHolder.getDataSourceClient();
    }

    private void loadInitialConfig() {
        try {
            T newValue = loadConfig();
            if (newValue == null) {
                RecordLog.warn("[EtcdDataSource] Initial configuration is null, you may have to check your data source");
            }
            getProperty().updateValue(newValue);
        } catch (Exception ex) {
            RecordLog.warn("[EtcdDataSource] Error when loading initial configuration", ex);
        }
    }

    private void initWatcher() {
        watcher = client.getWatchClient().watch(ByteSequence.from(key, charset), (watchResponse) -> {
            for (WatchEvent watchEvent : watchResponse.getEvents()) {
                WatchEvent.EventType eventType = watchEvent.getEventType();
                if (eventType == WatchEvent.EventType.PUT) {
                    try {
                        T newValue = loadConfig();
                        getProperty().updateValue(newValue);
                    } catch (Exception e) {
                        RecordLog.warn("[EtcdDataSource] Failed to update config", e);
                    }
                } else if (eventType == WatchEvent.EventType.DELETE) {
                    RecordLog.info("[EtcdDataSource] Cleaning config for key <{}>", key);
                    getProperty().updateValue(null);
                }
            }
        });
    }

    @Override
    public String readSource() throws Exception {
        CompletableFuture<GetResponse> responseFuture = client.getKVClient().get(ByteSequence.from(key, charset));
        List<KeyValue> kvs = responseFuture.get().getKvs();
        return kvs.size() == 0 ? null : kvs.get(0).getValue().toString(charset);
    }

    @Override
    public void close() {
        if (watcher != null) {
            try {
                watcher.close();
            } catch (Exception ex) {
                RecordLog.info("[EtcdDataSource] Failed to close watcher", ex);
            }
        }
    }
}
