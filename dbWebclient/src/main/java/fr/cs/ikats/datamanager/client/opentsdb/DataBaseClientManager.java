/**
 * Copyright 2018 CS Systèmes d'Information
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.cs.ikats.datamanager.client.opentsdb;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import fr.cs.ikats.util.configuration.ConfigProperties;
import fr.cs.ikats.util.configuration.IkatsConfiguration;

/**
 * URL builder class for openTSDB
 */
@Component("DataBaseClientManager")
@Scope("singleton")
@Lazy
public class DataBaseClientManager {

    private static final Logger LOGGER = Logger.getLogger(DataBaseClientManager.class);

    /**
     * URL base string for lookup operation
     */
    private String _lookupUrlBase;

    /**
     * URL base string for query operation
     */
    private String _queryUrlBase;

    private String _uidMetaUrlBase;

    private String _useMsResolution;

    private String _queryLastUrlBase;

    private IkatsConfiguration<ApplicationConfiguration> config = new IkatsConfiguration<ApplicationConfiguration>(ApplicationConfiguration.class);

    enum ApplicationConfiguration implements ConfigProperties {

        URL_DB_API_LOOKUP("url.db.api.lookup"),
        URL_DB_API_QUERY("url.db.api.query"),
        DB_API_MSRESOLUTION("db.api.msResolution"),
        URL_DB_API_UID_META("url.db.api.uid.tsmeta"),
        URL_DB_API_QUERY_LAST("url.db.api.query.last");

        // Filename
        public final static String propertiesFile = "api.properties";

        private String propertyName;
        private String defaultValue;

        ApplicationConfiguration(String propertyName) {
            this.propertyName = propertyName;
            this.defaultValue = null;
        }

        public String getPropertiesFilename() {
            return propertiesFile;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public String getDefaultValue() {
            return defaultValue;
        }
    }

    /**
     * Getter
     *
     * @return the _lookupUrlBase
     */
    public String getLookupUrlBase() {
        if (_lookupUrlBase == null) {
            _lookupUrlBase = (String) config.getProperty(ApplicationConfiguration.URL_DB_API_LOOKUP);
        }
        return _lookupUrlBase;
    }

    /**
     * Getter
     *
     * @return the _queryUrlBase
     */
    public String getQueryUrlBase() {
        if (_queryUrlBase == null) {
            _queryUrlBase = (String) config.getProperty(ApplicationConfiguration.URL_DB_API_QUERY);
        }
        return _queryUrlBase;
    }

    /**
     * Getter
     *
     * @return _useMsResolution
     */
    public String getMsResolution() {
        if (_useMsResolution == null) {
            _useMsResolution = (String) config.getProperty(ApplicationConfiguration.DB_API_MSRESOLUTION);
        }
        return _useMsResolution;
    }


    /**
     * Getter
     *
     * @return the _uidMetaUrlBase
     */
    public String getUidMetaUrlBase() {
        if (_uidMetaUrlBase == null) {
            _uidMetaUrlBase = (String) config.getProperty(ApplicationConfiguration.URL_DB_API_UID_META);
        }
        return _uidMetaUrlBase;
    }

    /**
     * get configuration param QueryLastUrl
     *
     * @return a string
     */
    public String getQueryLastUrlBase() {
        if (_queryLastUrlBase == null) {
            _queryLastUrlBase = (String) config.getProperty(ApplicationConfiguration.URL_DB_API_QUERY_LAST);
        }
        return _queryLastUrlBase;
    }

    /**
     * default constructor
     */
    public DataBaseClientManager() {

    }

    /**
     * generate a lookup request for the given metric and query
     *
     * @param metric      metric asked
     * @param queryParams a multi value map of params for query
     * @return the formated URL
     * @throws UnsupportedEncodingException if request URL cannot be encoded
     */
    public String generateLookupRequest(String metric, MultivaluedMap<String, String> queryParams) throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder(getLookupUrlBase());
        sb.append(metric);

        if (queryParams != null && !queryParams.isEmpty()) {

            sb.append(URLEncoder.encode("{", "UTF-8"));
            for (String tagName : queryParams.keySet()) {
                for (String value : queryParams.get(tagName)) {
                    if (queryParams.get(tagName).indexOf(value) != 0) {
                        sb.append(",");
                    }
                    sb.append(tagName).append("%3D").append(value);
                }
            }
            sb.append(URLEncoder.encode("}", "UTF-8"));
        }
        return sb.toString();
    }

    /**
     * generate a UIDMeta request for a tsuid
     *
     * @param tsuid the ts
     * @return a string
     */
    public String generateUIDMetaRequest(String tsuid) {
        StringBuilder sb = new StringBuilder(getUidMetaUrlBase());
        sb.append(tsuid);
        return sb.toString();
    }

    /**
     * generate a string , according to the following format :
     * <code>
     * m=&lt;aggregator&gt;:[&lt;down_sampler &gt;:]&lt;metric_name&gt;[{&lt;tag_name1&gt;=&lt;grouping
     * filter&gt;[,...&lt;tag_nameN&gt;=&lt;grouping_filter&gt;]}][{&lt;tag_name1&gt;=&lt;non grouping
     * filter&gt;[,...&lt;tag_nameN&gt;=&lt;non_grouping_filter&gt;]}] = new
     * DataBaseClientManager(
     * </code>
     *
     * @param aggregator           : one value of
     *                             ["min","mimmin","max","mimmax","dev","sum","avg"]
     * @param downSampler          downsampling method
     * @param downSamplerPeriod    period
     * @param metricName           metric
     * @param tagNameValueCriteria tags criteria
     * @return the url query part for metric request
     */
    public String generateMetricQueryForQueryRequest(String aggregator, String downSampler, String downSamplerPeriod, String metricName,
                                                     String tagNameValueCriteria) {
        StringBuilder sb = new StringBuilder("m=");
        if (aggregator != null) {
            sb.append(aggregator).append(":");
        } else {
            sb.append("sum:");
        }

        if (downSampler != null) {
            sb.append(downSamplerPeriod).append("-").append(downSampler).append(":");
        }
        sb.append(metricName);
        if (tagNameValueCriteria != null) {
            try {
                sb.append(URLEncoder.encode(tagNameValueCriteria, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("", e);
            }
        }
        return sb.toString();
    }

    /**
     * generate a query url param for metric and tags
     *
     * @param metricName the metric
     * @param tags       the tags
     * @return the url query part for metric request
     */
    public String generateMetricLastQueryUrl(String metricName, String tags) {
        StringBuilder sb = new StringBuilder(getQueryLastUrlBase()).append("timeseries=");
        sb.append(metricName);
        try {
            sb.append(URLEncoder.encode(tags, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("", e);
        }
        return sb.toString();
    }

    /**
     * build the request as
     * <code>
     * 'start=START_TIME&amp;m=avg:metrique{numero=00001|00002|....}' with following chars encoded
     * : {=%7B, }=%7D, |=%7C
     * </code>
     *
     * @param metricName        metric
     * @param tags              tags criteria
     * @param aggregator        : one value of
     *                          ["min","mimmin","max","mimmax","dev","sum","avg"]
     * @param downSampler       downsampling method
     * @param downSamplerPeriod period
     * @param startTime         start time
     * @param endTime           end time
     * @param urlOptions        other url option
     * @return url query part for metric request
     * @throws IkatsWebClientException if URL cannot be encoded
     */
    public String generateMetricQueryUrl(String metricName, String tags, String aggregator, String downSampler, String downSamplerPeriod,
                                         String startTime, String endTime, String urlOptions) throws IkatsWebClientException {
        StringBuilder sb = new StringBuilder(getQueryUrlBase() + "ms=" + getMsResolution() + "&start=");
        sb.append(startTime);
        if (endTime != null) {
            sb.append("&end=").append(endTime);
        }
        sb.append("&").append(generateMetricQueryForQueryRequest(aggregator, downSampler, downSamplerPeriod, metricName, tags));
        if (urlOptions != null) {
            sb.append("&" + urlOptions);
        }
        return sb.toString();
    }

    /**
     * build the url query param for TSUID requests
     *
     * @param tsuids            the tsuid list
     * @param aggregator        the aggregator metode, if null then, sum is used
     * @param startTime         the start time
     * @param endTime           the end time
     * @param urlOptions        other url options
     * @param downSampler       downsampling
     * @param downSamplerPeriod the period
     * @return url query part for TSUID request
     */
    public String generateQueryTSUIDUrl(final List<String> tsuids, String aggregator, String startTime, String endTime, String urlOptions, String downSampler, String downSamplerPeriod) {
        StringBuilder sb = new StringBuilder(getQueryUrlBase() + "ms=" + getMsResolution() + "&start=");
        sb.append(startTime);
        if (endTime != null) {
            sb.append("&end=").append(endTime);
        }
        if (urlOptions != null) {
            sb.append("&" + urlOptions);
        }
        sb.append("&tsuid=");
        if (aggregator != null) {
            sb.append(aggregator + ":");
        } else {
            sb.append("sum:");
        }
        if ((downSampler != null) && (downSamplerPeriod != null)) {
            sb.append(downSamplerPeriod + '-' + downSampler + ':');
        }
        for (Iterator<String> iter = tsuids.iterator(); iter.hasNext(); ) {
            String uid = iter.next();
            sb.append(uid);
            if (iter.hasNext()) {
                sb.append(",");
            }
        }

        return sb.toString();
    }

    /**
     * get a client with JSON feature activated and MultiPart
     *
     * @return he client
     */
    public Client getClientWithJSONFeature() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(MultiPartFeature.class).register(JacksonFeature.class);
        clientConfig.register(LoggingFilter.class);
        JerseyClient client = JerseyClientBuilder.createClient(clientConfig);
        return client;
    }
}
