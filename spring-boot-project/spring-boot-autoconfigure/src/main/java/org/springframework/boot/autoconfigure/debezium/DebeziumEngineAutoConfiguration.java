/*
 * Copyright 2023-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.boot.autoconfigure.debezium;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Consumer;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.engine.format.SerializationFormat;
import io.debezium.engine.spi.OffsetCommitPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.debezium.DebeziumProperties.DebeziumFormat;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Debezium embedded Engine.
 *
 * The engine configuration is standalone and only talks with the source data system;
 *
 * Applications using the engine auto-configuration should provides a {@link Consumer
 * consumer function} implementation to which the engine will pass all records containing
 * database change events.
 *
 * <pre>
 *  {@code
 * &#64;Bean
 * public Consumer<ChangeEvent<byte[], byte[]>> changeEventConsumer() {
 * 	return new Consumer<ChangeEvent<byte[], byte[]>>() {
 * 		&#64;Override
 * 		public void accept(ChangeEvent<byte[], byte[]> changeEvent) {
 * 			// Your code
 * 		}
 * 	};
 * }
 * }
 * </pre>
 *
 * <p>
 * With the engine, the application that runs the connector assumes all responsibility for
 * fault tolerance, scalability, and durability. Additionally, applications must specify
 * how the engine can store its relational database schema history and offsets. By
 * default, this information will be stored in memory and will thus be lost upon
 * application restart.
 *
 * The {@link DebeziumEngine.Builder} is activated only if a Debezium connector is
 * provided on the classpath and the
 * <code> spring.debezium.properties.connector.class</code>
 *
 * property is set.
 *
 * The Debezium Engine is designed to be submitted to an {@link Executor} or
 * {@link ExecutorService} for execution by a single thread.
 *
 * The Debezium auto-configuration provides a default {@DebeziumExecutorService} that can
 * be disabled and overridden with
 * <code>spring.debezium.executionServiceEnabled=false</code> property.
 *
 * All properties with prefix <code>spring.debezium.properties</code> are passed through
 * as native Debezium properties.
 *
 * @author Christian Tzolov
 * @since 3.1.0
 */
@AutoConfiguration
@EnableConfigurationProperties(DebeziumProperties.class)
@Conditional(DebeziumEngineAutoConfiguration.OnDebeziumConnectorCondition.class)
@ConditionalOnProperty(prefix = "spring.debezium", name = "properties.connector.class")
public class DebeziumEngineAutoConfiguration {

	private static final Log logger = LogFactory.getLog(DebeziumEngineAutoConfiguration.class);

	/**
	 * The fully-qualified class name of the commit policy type. The default is a periodic
	 * commit policy based upon time intervals.
	 * @param properties The 'debezium.properties.offset.flush.interval.ms' configuration
	 * is compulsory for the Periodic policy type. The ALWAYS and DEFAULT doesn't require
	 * properties.
	 */
	@Bean
	@ConditionalOnMissingBean
	public OffsetCommitPolicy offsetCommitPolicy(DebeziumProperties properties) {

		switch (properties.getOffsetCommitPolicy()) {
			case PERIODIC:
				return OffsetCommitPolicy.periodic(properties.getDebeziumNativeConfiguration());
			case ALWAYS:
				return OffsetCommitPolicy.always();
			case DEFAULT:
			default:
				return NULL_OFFSET_COMMIT_POLICY;
		}
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(prefix = "spring.debezium", name = "executionServiceEnabled", havingValue = "true",
			matchIfMissing = true)
	public DebeziumExecutorService embeddedEngine(
			DebeziumEngine.Builder<ChangeEvent<byte[], byte[]>> debeziumEngineBuilder) {
		return new DebeziumExecutorService(debeziumEngineBuilder.build());
	}

	@Bean
	@ConditionalOnMissingBean
	public Consumer<ChangeEvent<byte[], byte[]>> changeEventConsumer() {
		logger.warn("Default, dummy Debezium consumer! \n"
				+ "You should provide your own Consumer<ChangeEvent<byte[], byte[]>> implementation bean instead!");
		return new Consumer<ChangeEvent<byte[], byte[]>>() {
			@Override
			public void accept(ChangeEvent<byte[], byte[]> changeEvent) {
				logger.info("Change event:" + changeEvent);
			}
		};
	}

	/**
	 * Use the specified clock when needing to determine the current time. Defaults to
	 * {@link Clock#systemDefaultZone() system clock}, but you can override the Bean in
	 * your configuration with you {@link Clock implementation}. Returns
	 * @return Clock for the system default zone.
	 */
	@Bean
	@ConditionalOnMissingBean
	public Clock debeziumClock() {
		return Clock.systemDefaultZone();
	}

	/**
	 * When the engine's {@link DebeziumEngine#run()} method completes, call the supplied
	 * function with the results.
	 * @return Default completion callback that logs the completion status. The bean can
	 * be overridden in custom implementation.
	 */
	@Bean
	@ConditionalOnMissingBean
	public CompletionCallback completionCallback() {
		return DEFAULT_COMPLETION_CALLBACK;
	}

	/**
	 * During the engine run, provides feedback about the different stages according to
	 * the completion state of each component running within the engine (connectors, tasks
	 * etc). The bean can be overridden in custom implementation.
	 */
	@Bean
	@ConditionalOnMissingBean
	public ConnectorCallback connectorCallback() {
		return DEFAULT_CONNECTOR_CALLBACK;
	}

	@Bean
	public DebeziumEngine.Builder<ChangeEvent<byte[], byte[]>> debeziumEngineBuilder(
			Consumer<ChangeEvent<byte[], byte[]>> changeEventConsumer, OffsetCommitPolicy offsetCommitPolicy,
			CompletionCallback completionCallback, ConnectorCallback connectorCallback, DebeziumProperties properties,
			Clock debeziumClock) {

		return DebeziumEngine.create(this.serializationFormat(properties.getFormat()))
			.using(properties.getDebeziumNativeConfiguration())
			.using(debeziumClock)
			.using(completionCallback)
			.using(connectorCallback)
			.using((offsetCommitPolicy != NULL_OFFSET_COMMIT_POLICY) ? offsetCommitPolicy : null)
			.notifying(changeEventConsumer);
	}

	/**
	 * Helper to convert the desired data format serialization.
	 * @param format Debezium data format.
	 * @return Returns the serialization format class for the requested format.
	 */
	private Class<? extends SerializationFormat<byte[]>> serializationFormat(DebeziumFormat format) {
		switch (format) {
			case JSON:
				return io.debezium.engine.format.JsonByteArray.class;
			case AVRO:
				return io.debezium.engine.format.Avro.class;
			case PROTOBUF:
				return io.debezium.engine.format.Protobuf.class;
			default:
				throw new IllegalStateException("Unknown Debezium format: " + format);
		}
	}

	/**
	 * A callback function to be notified when the connector completes.
	 */
	private static final CompletionCallback DEFAULT_COMPLETION_CALLBACK = new CompletionCallback() {
		@Override
		public void handle(boolean success, String message, Throwable error) {
			logger.warn(String.format("[DEFAULT HANDLER] Debezium Engine handle with success:%s, message:%s ", success,
					message), error);
		}
	};

	/**
	 * Callback function which informs users about the various stages a connector goes
	 * through during startup.
	 */
	private static final ConnectorCallback DEFAULT_CONNECTOR_CALLBACK = new ConnectorCallback() {

		/**
		 * Called after a connector has been successfully started by the engine.
		 */
		public void connectorStarted() {
			logger.info("Connector Started!");
		};

		/**
		 * Called after a connector has been successfully stopped by the engine.
		 */
		public void connectorStopped() {
			logger.info("Connector Stopped!");
		}

		/**
		 * Called after a connector task has been successfully started by the engine.
		 */
		public void taskStarted() {
			logger.info("Connector Task Started!");
		}

		/**
		 * Called after a connector task has been successfully stopped by the engine.
		 */
		public void taskStopped() {
			logger.info("Connector Task Stopped!");
		}

	};

	/**
	 * The policy that defines when the offsets should be committed to offset storage.
	 */
	private static final OffsetCommitPolicy NULL_OFFSET_COMMIT_POLICY = new OffsetCommitPolicy() {
		@Override
		public boolean performCommit(long numberOfMessagesSinceLastCommit, Duration timeSinceLastCommit) {
			throw new UnsupportedOperationException("Unimplemented method 'performCommit'");
		}
	};

	/**
	 * Determine if Debezium connector is available. This either kicks in if any debezium
	 * connector is available.
	 */
	@Order(Ordered.LOWEST_PRECEDENCE)
	static class OnDebeziumConnectorCondition extends AnyNestedCondition {

		OnDebeziumConnectorCondition() {
			super(ConfigurationPhase.REGISTER_BEAN);
		}

		@ConditionalOnClass(name = { "io.debezium.connector.mysql.MySqlConnector" })
		static class HasMySqlConnector {

		}

		@ConditionalOnClass(name = "io.debezium.connector.postgresql.PostgresConnector")
		static class HasPostgreSqlConnector {

		}

		@ConditionalOnClass(name = "io.debezium.connector.db2.Db2Connector")
		static class HasDb2Connector {

		}

		@ConditionalOnClass(name = "io.debezium.connector.oracle.OracleConnector")
		static class HasOracleConnector {

		}

		@ConditionalOnClass(name = "io.debezium.connector.sqlserver.SqlServerConnector")
		static class HasSqlServerConnector {

		}

		@ConditionalOnClass(name = "io.debezium.connector.mongodb.MongoDbConnector")
		static class HasMongoDbConnector {

		}

		@ConditionalOnClass(name = "io.debezium.connector.vitess.VitessConnector")
		static class HasVitessConnector {

		}

		@ConditionalOnClass(name = "io.debezium.connector.spanner.SpannerConnector")
		static class HasSpannerConnector {

		}

	}

}
