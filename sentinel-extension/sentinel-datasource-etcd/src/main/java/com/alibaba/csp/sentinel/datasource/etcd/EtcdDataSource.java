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

import com.alibaba.csp.sentinel.datasource.DataSourceHolder;
import com.alibaba.csp.sentinel.datasource.DataSourceMode;
import com.alibaba.csp.sentinel.datasource.converter.SentinelConverter;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;

import java.nio.charset.Charset;

/**
 * @author Jiajiangnan
 */
public class EtcdDataSource<T> extends DataSourceHolder {

    private EtcdReadableDataSource<T> readableDataSource;
    private EtcdWritableDataSource<T> writableDataSource;

    /**
     * Create an etcd data-source. The connection configuration will be retrieved from {@link EtcdConfig}.
     *
     * @param key    config key
     * @param converter data converter
     */
    public EtcdDataSource(String key, SentinelConverter<String, T> converter) {
        this(key, converter, DataSourceMode.READABLE);
    }

    /**
     * Create an etcd data-source. The connection configuration will be retrieved from {@link EtcdConfig}.
     *
     * @param key    config key
     * @param converter data converter
     * @param dataSourceMode data dataSourceMode
     */
    public EtcdDataSource(String key, SentinelConverter<String, T> converter, DataSourceMode dataSourceMode) {
        super(converter, dataSourceMode);

        Charset charset = Charset.forName(EtcdConfig.getCharset());

        Client etcdClient = null;
        if (!EtcdConfig.isAuthEnable()) {
            etcdClient = Client.builder()
                .endpoints(EtcdConfig.getEndPoints().split(",")).build();
        } else {
            etcdClient = Client.builder()
                .endpoints(EtcdConfig.getEndPoints().split(","))
                .user(ByteSequence.from(EtcdConfig.getUser(), charset))
                .password(ByteSequence.from(EtcdConfig.getPassword(), charset))
                .authority(EtcdConfig.getAuthority())
                .build();
        }
        super.setDataSourceClient(etcdClient);


        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.READABLE == dataSourceMode) {
            this.readableDataSource = new EtcdReadableDataSource(key, charset, this);
        }

        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.WRITABLE == dataSourceMode) {
            this.writableDataSource = new EtcdWritableDataSource<>(key, charset, this);
        }

    }

    public EtcdReadableDataSource<T> getReader() {
        return this.readableDataSource;
    }

    public EtcdWritableDataSource<T> getWriter() {
        return this.writableDataSource;
    }

    public void close() throws Exception {
        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.READABLE == dataSourceMode) {
            this.readableDataSource.close();
        }

        if(DataSourceMode.ALL == dataSourceMode || DataSourceMode.WRITABLE == dataSourceMode) {
            this.writableDataSource.close();
        }

        if (this.getDataSourceClient() != null) {
            ((Client) this.getDataSourceClient()).close();
        }
    }

}
