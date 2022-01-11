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

import com.alibaba.csp.sentinel.datasource.AbstractWritableDataSource;
import com.alibaba.csp.sentinel.datasource.DataSourceHolder;
import com.alibaba.csp.sentinel.datasource.converter.SentinelConverter;
import com.ecwid.consul.v1.ConsulClient;

/**
 * @author Jiajiangnan
 */
public class ConsulWritableDataSource<T> extends AbstractWritableDataSource<T> {

    private final String ruleKey;
    private final SentinelConverter<String, T> sentinelConverter;

    private ConsulClient consulClient;

    public ConsulWritableDataSource(String ruleKey, DataSourceHolder dataSourceHolder) {
        super(dataSourceHolder);

        this.ruleKey = ruleKey;
        this.sentinelConverter = dataSourceHolder.getConverter();

        init();
    }

    private void init() {
        initConsulClient();
    }

    private void  initConsulClient() {
        this.consulClient = dataSourceHolder.getDataSourceClient() == null ? null : (ConsulClient) dataSourceHolder.getDataSourceClient();
    }

    @Override
    public void write(T value) throws Exception {

        String fromSentinel = sentinelConverter.fromSentinel(value);
        consulClient.setKVValue(ruleKey, fromSentinel);
    }

    @Override
    public void close() throws Exception {
        // Nothing to do
    }

}
