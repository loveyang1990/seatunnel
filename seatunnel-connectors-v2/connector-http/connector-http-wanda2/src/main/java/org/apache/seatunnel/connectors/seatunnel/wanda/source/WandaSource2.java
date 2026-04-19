/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.wanda.source;

import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.config.WandaSourceConfig2;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.config.WandaSourceParameter2;
import org.apache.seatunnel.connectors.seatunnel.wanda.source.http.WandaHttpSourceReader;
import org.apache.seatunnel.shade.com.typesafe.config.Config;


import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class WandaSource2 extends HttpSource {
    private final WandaSourceParameter2 wandaSourceParameter = new WandaSourceParameter2();
    private boolean contentKeyTolowercase = false;
    protected WandaSource2(ReadonlyConfig pluginConfig) {
        super(pluginConfig);

        List<Map<String, Object>> dbsourceResult = new ArrayList<>();
        if(pluginConfig.getOptional(Options.key("queryParaDbsource").stringType().noDefaultValue()).isPresent()) {
            Config config = pluginConfig.toConfig();
            Config dbsource = config.getConfig("queryParaDbsource");
            MySqlCatalog catalog =
                    new MySqlCatalog(
                            "mysql",
                            dbsource.getString("user"),
                            dbsource.getString("password"),
                            JdbcUrlUtil.getUrlInfo(dbsource.getString("url")), null);
            catalog.open();
            dbsourceResult = catalog.querySql(dbsource.getString("querySql"));
        }
        this.wandaSourceParameter.buildWithConfig(pluginConfig, dbsourceResult);

        // 先构建查询参数；
        if (pageInfo != null && pageInfo.getBatchSize() != null) {
            this.wandaSourceParameter.getParams().put("pageSize", String.valueOf(pageInfo.getBatchSize()));
        }


        if (pluginConfig.getOptional(WandaSourceConfig2.CONTENT_KEY_TOLOWERCASE).isPresent()) {
            this.contentKeyTolowercase = pluginConfig.get(WandaSourceConfig2.CONTENT_KEY_TOLOWERCASE);
        }

    }

    @Override
    public String getPluginName() {
        return "Wanda2";
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new HttpSourceReader(
                this.wandaSourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField, pageInfo, contentKeyTolowercase);
    }

}
