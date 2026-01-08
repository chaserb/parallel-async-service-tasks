package demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flowable.engine.HistoryService;
import org.flowable.engine.ManagementService;
import org.flowable.engine.ProcessEngine;
import org.flowable.engine.RepositoryService;
import org.flowable.engine.RuntimeService;
import org.flowable.eventregistry.api.EventRegistry;
import org.flowable.eventregistry.api.EventRepositoryService;
import org.flowable.eventregistry.api.InboundEventChannelAdapter;
import org.flowable.eventregistry.api.OutboundEventChannelAdapter;
import org.flowable.eventregistry.impl.EventRegistryEngine;
import org.flowable.eventregistry.model.InboundChannelModel;
import org.flowable.eventregistry.spring.EventRegistryFactoryBean;
import org.flowable.eventregistry.spring.SpringEventRegistryEngineConfiguration;
import org.flowable.spring.ProcessEngineFactoryBean;
import org.flowable.spring.SpringProcessEngineConfiguration;
import org.h2.Driver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;

import static org.flowable.common.engine.impl.interceptor.EngineConfigurationConstants.KEY_EVENT_REGISTRY_CONFIG;

/**
 * Test configuration for ParallelAsyncServiceTaskTest that provides
 * a minimal setup with both process engine and event registry.
 */
@Configuration
public class ParallelAsyncTestConfiguration {

    @Bean
    public DataSource dataSource() {
        final SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        dataSource.setDriverClass(Driver.class);
        dataSource.setUrl("jdbc:h2:mem:flowable-test;DB_CLOSE_DELAY=1000");
        dataSource.setUsername("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    @Bean
    public PlatformTransactionManager transactionManager(final DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public SpringProcessEngineConfiguration processEngineConfig(final DataSource dataSource,
            final PlatformTransactionManager transactionManager) {
        final SpringProcessEngineConfiguration config = new SpringProcessEngineConfiguration();
        config.setDataSource(dataSource);
        config.setTransactionManager(transactionManager);
        config.setDatabaseSchemaUpdate("drop-create");

        // CRITICAL: Set to false to prevent zombie executions
        config.setEventRegistryStartProcessInstanceAsync(false);

        // Enable event registry - REQUIRED for EventRegistryStartProcessInstanceAsync to work
        config.setDisableEventRegistry(false);

        return config;
    }

    @Bean
    public ProcessEngineFactoryBean processEngine(final SpringProcessEngineConfiguration processEngineConfig) {
        final ProcessEngineFactoryBean factoryBean = new ProcessEngineFactoryBean();
        factoryBean.setProcessEngineConfiguration(processEngineConfig);
        return factoryBean;
    }

    @Bean
    public SpringEventRegistryEngineConfiguration eventRegistryEngineConfig(final ProcessEngine processEngine) {
        return (SpringEventRegistryEngineConfiguration) processEngine.getProcessEngineConfiguration()
                .getEngineConfigurations().get(KEY_EVENT_REGISTRY_CONFIG);
    }

    @Bean
    public EventRegistryFactoryBean eventRegistryEngine(final SpringEventRegistryEngineConfiguration eventRegistryConfig) {
        final EventRegistryFactoryBean factoryBean = new EventRegistryFactoryBean();
        factoryBean.setEventEngineConfiguration(eventRegistryConfig);
        return factoryBean;
    }

    @Bean
    public RepositoryService repositoryService(final ProcessEngine processEngine) {
        return processEngine.getRepositoryService();
    }

    @Bean
    public RuntimeService runtimeService(final ProcessEngine processEngine) {
        return processEngine.getRuntimeService();
    }

    @Bean
    public HistoryService historyService(final ProcessEngine processEngine) {
        return processEngine.getHistoryService();
    }

    @Bean
    public ManagementService managementService(final ProcessEngine processEngine) {
        return processEngine.getManagementService();
    }

    @Bean
    public EventRepositoryService eventRepositoryService(final EventRegistryEngine eventRegistryEngine) {
        return eventRegistryEngine.getEventRepositoryService();
    }

    @Bean
    public EventRegistry eventRegistry(final EventRegistryEngine eventRegistryEngine) {
        return eventRegistryEngine.getEventRegistry();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public TestInboundChannel testInboundChannel() {
        return new TestInboundChannel();
    }

    /**
     * Simple test implementation of InboundEventChannelAdapter for triggering
     * events without MassTransit message serialization complexity. Just uses
     * simple JSON payloads. Also implements OutboundEventChannelAdapter to
     * handle events sent by serviceTasks.
     */
    public static class TestInboundChannel implements InboundEventChannelAdapter,
            OutboundEventChannelAdapter<String> {
        private InboundChannelModel inboundChannelModel;
        private EventRegistry eventRegistry;

        public void eventReceived(final String eventPayload) {
            eventRegistry.eventReceived(inboundChannelModel, eventPayload);
        }

        @Override
        public void setInboundChannelModel(final InboundChannelModel inboundChannelModel) {
            this.inboundChannelModel = inboundChannelModel;
        }

        @Override
        public void setEventRegistry(final EventRegistry eventRegistry) {
            this.eventRegistry = eventRegistry;
        }

        @Override
        public void sendEvent(String rawEvent, Map<String, Object> headerMap) {
            // In a real scenario, this would send to RabbitMQ or another
            // message broker. For tests, we just ignore outbound events sent by
            // serviceTasks. The tests must manually trigger response events
        }
    }

}
