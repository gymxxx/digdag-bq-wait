package io.digdag.plugin.gcp;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Table;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.core.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

class BqWaitClient extends BaseGcpClient<Bigquery>

{
    private static final Logger logger = LoggerFactory.getLogger(BqWaitClient.class);

    BqWaitClient(GoogleCredential credential, Optional<ProxyConfig> proxyConfig)
    {
        super(credential, proxyConfig);
    }

    @Override
    protected Bigquery client(GoogleCredential credential, HttpTransport transport, JsonFactory jsonFactory)
    {
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }

        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("Digdag")
                .build();
    }


    Optional<Table> getTable(String projectId, String datasetId, String tableId,  boolean rowsExist)
            throws IOException
    {
        try {
            Table ret = client.tables().get(projectId, datasetId, tableId).execute();
            if(rowsExist && ret.getNumRows().compareTo(BigInteger.ZERO) <= 0) {
                return Optional.absent();
            }
            return Optional.of(ret);
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                return Optional.absent();
            }
            throw e;
        }
    }
    static class Factory
            extends BaseGcpClient.Factory
    {
        @Inject
        public Factory(@Environment Map<String, String> environment)
        {
            super(environment);
        }

        BqWaitClient create(GoogleCredential credential)
        {
            return new BqWaitClient(credential, proxyConfig);
        }
    }
}