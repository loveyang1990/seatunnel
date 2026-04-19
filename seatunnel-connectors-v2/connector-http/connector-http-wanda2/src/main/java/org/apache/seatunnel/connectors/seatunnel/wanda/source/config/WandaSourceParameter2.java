
package org.apache.seatunnel.connectors.seatunnel.wanda.source.config;


import lombok.SneakyThrows;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class WandaSourceParameter2 extends HttpParameter {

    @SneakyThrows
    public void buildWithConfig(ReadonlyConfig pluginConfig, List<Map<String, Object>> dbStartPointSourceResultList)  {
        super.buildWithConfig(pluginConfig);
        // set url
//        // set method
        // set body
        Map<String, String> params = new HashMap<>();
        String appkey = pluginConfig.get(WandaSourceConfig2.APP_KEY);
        String appsecrt = pluginConfig.get(WandaSourceConfig2.APP_SECRET);
        String nowStr = DateTimeUtils.toString(LocalDateTime.now(), DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
        params.put("appKey", appkey);
        params.put("strSysDatetime", URLEncoder.encode(nowStr, "UTF-8"));
//        params.put("pageSize", String.valueOf(pluginConfig.getOptional(WandaSourceConfig2.PAGE_SIZE).get()));
        params.put("dataType", "json");
        params.put("queryPara", URLEncoder.encode(
                pluginConfig.getOptional(WandaSourceConfig2.queryPara).get(), "UTF-8"));
        String sign = DigestUtils.md5Hex(appkey + nowStr + appsecrt);
        params.put("sign", sign);
        if (CollectionUtils.isNotEmpty(dbStartPointSourceResultList)) {
            for (Map<String, Object> dbStartPointSourceResult : dbStartPointSourceResultList) {
                if (MapUtils.isNotEmpty(dbStartPointSourceResult)) {
                    String queryParaParam = pluginConfig.get(Options.key("queryPara").stringType().noDefaultValue());
                    Map<String, String> queryParamMap = JsonUtils.toMap(queryParaParam);
                    for (String queryParamKey : queryParamMap.keySet()) {
                        String queryParamValueStr = queryParamMap.get(queryParamKey);
                        queryParamValueStr = fillExpression(queryParamValueStr,dbStartPointSourceResult);
                        queryParamMap.put(queryParamKey, queryParamValueStr);
                    }

                    params.put("queryPara", URLEncoder.encode(JsonUtils.toJsonString(queryParamMap), "UTF-8"));
                }
            }
        }
        Map<String, Object> pageParams1 = this.getPageParams();


        this.setParams(params);
//        this.setRetryParameters(pluginConfig);
    }



    public static String fillExpression(String expression, Map<String, Object> valuesMap) {
        if (MapUtils.isEmpty(valuesMap)) {
            return expression;
        }
        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            String key = "${" + entry.getKey() + "}";
            if (entry.getValue() != null) {
                String value = entry.getValue().toString();
                expression = expression.replace(key, value);
            }

        }
        return expression;
    }
}
