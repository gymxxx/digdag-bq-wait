package io.digdag.plugin.gcp;


import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.model.Table;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.*;
import io.digdag.standards.operator.state.TaskState;

import java.nio.file.Path;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;

public class BqWaitOperatorFactory implements OperatorFactory {
    private final BqWaitClient.Factory clientFactory;
    private final GcpCredentialProvider credentialProvider;

    @Inject
    BqWaitOperatorFactory(
            BqWaitClient.Factory clientFactory,
            GcpCredentialProvider credentialProvider)
    {
        this.clientFactory = clientFactory;
        this.credentialProvider = credentialProvider;
    }
    @Override
    public String getType() {
        return "bq_wait";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request) {
        return new BqWaitOperator(projectPath, request);
    }

    private class BqWaitOperator
            extends BaseBqOperator
    {
        private final TaskState state;

        BqWaitOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request, clientFactory, credentialProvider);
            this.state = TaskState.of(request);
        }

        @Override
        protected TaskResult run(TaskExecutionContext ctx, BqWaitClient bqClient, String projectId)
        {
            Optional<String> datasetId = params.getOptional("dataset", String.class);
            Optional<String> tableId = params.getOptional("table", String.class);
            Optional<Boolean> rowsExist = params.getOptional("rows_exist", Boolean.class);

            if (!datasetId.isPresent() || !tableId.isPresent()) {
                throw new ConfigException("Either the bq_wait operator both 'dataset' and 'table' parameters must be set");
            }

            return await(bqClient, projectId, datasetId.get(), tableId.get(),rowsExist.or(true));
        }

        private TaskResult await(BqWaitClient bqClient, String projectId, String datasetId, String tableId, boolean rowsExist)
        {
            Table metadata = pollingWaiter(state, "exists")
                    .withWaitMessage("'%s:%s.%s' does not yet exist", projectId, datasetId, tableId)
                    .await(pollState -> pollingRetryExecutor(pollState, "poll")
                            .retryUnless(GoogleJsonResponseException.class, Gcp::isDeterministicException)
                            .run(s -> bqClient.getTable(projectId, datasetId, tableId, rowsExist)));

            return TaskResult.defaultBuilder(request)
                    .resetStoreParams(ImmutableList.of(ConfigKey.of("bq_wait", "last_object")))
                    .storeParams(storeParams(metadata))
                    .build();
        }

        private Config storeParams(Table metadata)
        {
            Config params = request.getConfig().getFactory().create();
            Config object = params.getNestedOrSetEmpty("bq_wait").getNestedOrSetEmpty("last_object");
            object.set("metadata", metadata);
            return params;
        }

    }
}
