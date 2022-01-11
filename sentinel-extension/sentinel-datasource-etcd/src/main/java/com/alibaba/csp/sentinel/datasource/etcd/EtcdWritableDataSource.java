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

import com.alibaba.csp.sentinel.datasource.AbstractWritableDataSource;
import com.alibaba.csp.sentinel.datasource.DataSourceHolder;
import com.alibaba.csp.sentinel.datasource.converter.SentinelConverter;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;

import java.nio.charset.Charset;

/**
 * @author Jiajiangnan
 */
public class EtcdWritableDataSource<T> extends AbstractWritableDataSource<T> {

    private final String key;
    private final Charset charset;
    private final DataSourceHolder dataSourceHolder;
    private final SentinelConverter<String, T> converter;

    private Client client;

    public EtcdWritableDataSource(String key, Charset charset, DataSourceHolder dataSourceHolder) {
        super(dataSourceHolder);

        this.key = key;
        this.charset = charset;
        this.dataSourceHolder = dataSourceHolder;

        this.converter = dataSourceHolder.getConverter();
        
        init();
    }

    private void init() {
        initClient();
    }

    private void initClient() {
        this.client = dataSourceHolder.getDataSourceClient() == null ? null : (Client) dataSourceHolder.getDataSourceClient();
    }

    @Override
    public void write(T value) throws Exception {
        String fromSentinel = converter.fromSentinel(value);
        client.getKVClient().put(ByteSequence.from(key, charset), ByteSequence.from(fromSentinel, charset));
    }

    @Override
    public void close() throws Exception {
        // Nothing to do
    }

}
