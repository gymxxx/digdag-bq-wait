package io.digdag.plugin.gcp;

import io.digdag.client.config.Config;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;

abstract class BaseBqOperator
        extends BaseGcpOperator
{
    private final BqWaitClient.Factory clientFactory;

    protected final Config params;

    protected BaseBqOperator(Path projectPath, TaskRequest request, BqWaitClient.Factory clientFactory, GcpCredentialProvider credentialProvider)
    {
        super(projectPath, request, credentialProvider);
        this.clientFactory = clientFactory;
        this.params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));
    }

    @Override
    protected TaskResult run(TaskExecutionContext ctx, GcpCredential credential, String projectId)
    {
        try (BqWaitClient bq = clientFactory.create(credential.credential())) {
            return run(ctx, bq, projectId);
        }
    }

    protected abstract TaskResult run(TaskExecutionContext ctx, BqWaitClient bq, String projectId);
}
