package io.digdag.plugin.gcp;

import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import io.digdag.spi.Plugin;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;

public class BqWaitPlugin
        implements Plugin
{
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type)
    {
        if (type == OperatorProvider.class) {
            return BqWaitOperatorProvider.class.asSubclass(type);
        }
        else {
            return null;
        }
    }

    private static class BqWaitOperatorProvider
            implements OperatorProvider
    {
        @Inject
        BqWaitClient.Factory clientFactory;
        @Inject
        GcpCredentialProvider credentialProvider;

        @Override
        public List<OperatorFactory> get()
        {
            return Collections.singletonList(
                    new BqWaitOperatorFactory(clientFactory, credentialProvider));
        }
    }
}