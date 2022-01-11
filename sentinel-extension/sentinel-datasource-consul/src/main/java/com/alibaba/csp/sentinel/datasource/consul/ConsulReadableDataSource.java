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
package com.alibaba.csp.sentinel.datasource.consul;

import com.alibaba.csp.sentinel.concurrent.NamedThreadFactory;
import com.alibaba.csp.sentinel.datasource.AbstractReadableDataSource;
import com.alibaba.csp.sentinel.datasource.DataSourceHolder;
import com.alibaba.csp.sentinel.datasource.converter.SentinelConverter;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.util.StringUtil;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A read-only {@code DataSource} with Consul backend.
 * <p>
 * <p>
 * The data source first initial rules from a Consul during initialization.
 * Then it start a watcher to observe the updates of rule date and update to memory.
 *
 * Consul do not provide http api to watch the update of KV，so it use a long polling and
 * <a href="https://www.consul.io/api/features/blocking.html">blocking queries</a> of the Consul's feature
 * to watch and update value easily.When Querying data by index will blocking until change or timeout. If
 * the index of the current query is larger than before, it means that the data has changed.
 * </p>
 *
 * @author wavesZh
 * @author Zhiguo.Chen
 * @author Jiajiangnan
 */
public class ConsulReadableDataSource<T> extends AbstractReadableDataSource<String, T> {

    private final String address;
    private final String token;
    private final String ruleKey;
    private final int watchTimeout;
    private final boolean autoRefresh;

    private final SentinelConverter<String, T> converter;

    private ConsulClient consulClient;
    private ConsulKVWatcher watcher;

    private ExecutorService watcherService;

    /**
     * Request of query will hang until timeout (in second) or get updated value.
     */
    private static final int WATCHT_IMEOUT = 5;

    /**
     * Record the data's index in Consul to watch the change.
     * If lastIndex is smaller than the index of next query, it means that rule data has updated.
     */
    private volatile long lastIndex;

    public ConsulReadableDataSource(String address, String token, String ruleKey, DataSourceHolder dataSourceHolder){
        this(address, token, ruleKey, WATCHT_IMEOUT, dataSourceHolder, true);
    }

    public ConsulReadableDataSource(String address, String token, String ruleKey, int watchTimeout, DataSourceHolder dataSourceHolder){
        this(address, token, ruleKey, watchTimeout, dataSourceHolder, true);
    }

    public ConsulReadableDataSource(String address, String token, String ruleKey, int watchTimeout, DataSourceHolder dataSourceHolder, final boolean autoRefresh) {
        super(dataSourceHolder);

        this.address = address;
        this.token = token;
        this.ruleKey = ruleKey;
        this.watchTimeout = watchTimeout;
        this.autoRefresh = autoRefresh;

        this.converter = dataSourceHolder.getConverter();

        init();
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    private void init() {
        initConsulClient();
        if(autoRefresh) {
            watcherService = Executors.newSingleThreadExecutor(new NamedThreadFactory("sentinel-consul-ds-watcher", true));
            watcher = new ConsulKVWatcher();

            startKVWatcher();
        }
        loadInitialConfig();
    }

    private void  initConsulClient() {
        this.consulClient = dataSourceHolder.getDataSourceClient() == null ? null : (ConsulClient) dataSourceHolder.getDataSourceClient();
    }

    private void startKVWatcher() {
        watcherService.submit(watcher);
    }

    private void loadInitialConfig() {
        try {
            T newValue = loadConfig();
            if (newValue == null) {
                RecordLog.warn(
                        "[ConsulDataSource] WARN: initial config is null, you may have to check your data source");
            }
            getProperty().updateValue(newValue);
        } catch (Exception ex) {
            RecordLog.warn("[ConsulDataSource] Error when loading initial config", ex);
        }
    }


    private class ConsulKVWatcher implements Runnable {
        private volatile boolean running = true;

        @Override
        public void run() {
            while (running) {
                // It will be blocked until watchTimeout(s) if rule data has no update.
                Response<GetValue> response = getValue(ruleKey, lastIndex, watchTimeout);
                if (response == null) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(watchTimeout * 1000);
                    } catch (InterruptedException e) {
                    }
                    continue;
                }
                GetValue getValue = response.getValue();
                Long currentIndex = response.getConsulIndex();
                if (currentIndex == null || currentIndex <= lastIndex) {
                    continue;
                }
                lastIndex = currentIndex;
                if (getValue != null) {
                    String newValue = getValue.getDecodedValue();
                    try {
                        getProperty().updateValue(converter.toSentinel(newValue));
                        RecordLog.info("[ConsulDataSource] New property value received for ({}, {}): {}", address, ruleKey, newValue);
                    } catch (Exception ex) {
                        // In case of parsing error.
                        RecordLog.warn("[ConsulDataSource] Failed to update value for ({}, {}), raw value: {}", address, ruleKey, newValue);
                    }
                }
            }
        }

        private void stop() {
            running = false;
        }
    }


    /**
     * Get data from Consul immediately.
     *
     * @param key data key in Consul
     * @return the value associated to the key, or null if error occurs
     */
    private Response<GetValue> getValueImmediately(String key) {
        return getValue(key, -1, -1);
    }

    /**
     * Get data from Consul (blocking).
     *
     * @param key      data key in Consul
     * @param index    the index of data in Consul.
     * @param waitTime time(second) for waiting get updated value.
     * @return the value associated to the key, or null if error occurs
     */
    private Response<GetValue> getValue(String key, long index, long waitTime) {
        try {
            if (StringUtil.isNotBlank(token)) {
                return consulClient.getKVValue(key, token, new QueryParams(waitTime, index));
            } else {
                return consulClient.getKVValue(key, new QueryParams(waitTime, index));
            }
        } catch (Throwable t) {
            RecordLog.warn("[ConsulDataSource] Failed to get value for key: " + key, t);
        }
        return null;
    }

    @Override
    public String readSource() throws Exception {
        if (this.consulClient == null) {
            throw new IllegalStateException("Consul has not been initialized or error occurred");
        }
        Response<GetValue> response = getValueImmediately(ruleKey);
        if (response != null) {
            GetValue value = response.getValue();
            lastIndex = response.getConsulIndex();
            return value != null ? value.getDecodedValue() : null;
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        watcher.stop();
        watcherService.shutdown();
    }

}
