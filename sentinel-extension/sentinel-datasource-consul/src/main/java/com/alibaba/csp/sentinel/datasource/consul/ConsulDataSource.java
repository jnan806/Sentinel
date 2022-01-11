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

import com.alibaba.csp.sentinel.datasource.DataSourceHolder;
import com.alibaba.csp.sentinel.datasource.DataSourceMode;
import com.alibaba.csp.sentinel.datasource.converter.SentinelConverter;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.ecwid.consul.v1.ConsulClient;

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
 */
public class ConsulDataSource<T> extends DataSourceHolder {

    private ConsulReadableDataSource<T> readableDataSource;
    private ConsulWritableDataSource<T> writableDataSource;

    private static final int DEFAULT_PORT = 8500;

    public ConsulDataSource(String host, String ruleKey, int watchTimeoutInSecond, SentinelConverter<String, T> converter) {
        this(host, DEFAULT_PORT, ruleKey, watchTimeoutInSecond, converter);
    }

    public ConsulDataSource(String host, String ruleKey, int watchTimeoutInSecond, SentinelConverter<String, T> converter, DataSourceMode dataSourceMode) {
        this(host, DEFAULT_PORT, ruleKey, watchTimeoutInSecond, converter, DataSourceMode.READABLE);
    }

    public ConsulDataSource(String host, int port, String ruleKey, int watchTimeout, SentinelConverter<String, T> converter) {
        this(host, port, null, ruleKey, watchTimeout, converter, DataSourceMode.READABLE);
    }
    public ConsulDataSource(String host, int port, String ruleKey, int watchTimeout, SentinelConverter<String, T> converter, DataSourceMode dataSourceMode) {
        this(host, port, null, ruleKey, watchTimeout, converter, dataSourceMode);
    }

    /**
     * Constructor of {@code ConsulDataSource}.
     *
     * @param host         consul agent host
     * @param port         consul agent port
     * @param token        consul agent acl token
     * @param ruleKey      data key in Consul
     * @param watchTimeout request for querying data will be blocked until new data or timeout. The unit is second (s)
     * @param converter    customized data parser, cannot be empty
     * @param dataSourceMode
     */
    public ConsulDataSource(String host, int port, String token, String ruleKey, int watchTimeout, SentinelConverter<String, T> converter, DataSourceMode dataSourceMode) {
        super(converter, dataSourceMode);
        AssertUtil.notNull(host, "Consul host can not be null");
        AssertUtil.notEmpty(ruleKey, "Consul ruleKey can not be empty");
        AssertUtil.isTrue(watchTimeout >= 0, "watchTimeout should not be negative");

        super.setDataSourceClient( new ConsulClient(host, port));

        String address = host + ":" + port;

        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.READABLE == dataSourceMode) {
            this.readableDataSource = new ConsulReadableDataSource(address, token, ruleKey, this);
        }

        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.WRITABLE == dataSourceMode) {
            this.writableDataSource = new ConsulWritableDataSource<>(ruleKey,this);
        }

    }

    public ConsulReadableDataSource<T> getReader() {
        return this.readableDataSource;
    }

    public ConsulWritableDataSource<T> getWriter() {
        return this.writableDataSource;
    }

    public void close() throws Exception {
        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.READABLE == dataSourceMode) {
            this.readableDataSource.close();
        }

        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.WRITABLE == dataSourceMode) {
            this.writableDataSource.close();
        }
    }

}
