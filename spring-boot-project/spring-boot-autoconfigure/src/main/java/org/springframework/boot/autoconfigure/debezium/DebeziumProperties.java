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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Christian Tzolov
 */
@ConfigurationProperties("spring.debezium")
public class DebeziumProperties {

	public enum DebeziumFormat {
		/**
		 * JSON change event format.
		 */
		JSON,
		/**
		 * AVRO change event format.
		 */
		AVRO,
		/**
		 * ProtoBuf change event format.
		 */
		PROTOBUF
	};

	/**
	 * Spring pass-trough wrapper for debezium configuration properties. All properties with a
	 * 'spring.debezium.properties.*' prefix are native Debezium properties.
	 */
	private Map<String, String> properties = new HashMap<>();

	/**
	 * Change Event message content format. Defaults to 'JSON'.
	 */
	private DebeziumFormat format = DebeziumFormat.JSON;

	/**
	 * Copy Change Event headers into Message headers.
	 */
	private boolean copyHeaders = true;

	/**
	 * The policy that defines when the offsets should be committed to offset storage.
	 */
	private DebeziumOffsetCommitPolicy offsetCommitPolicy = DebeziumOffsetCommitPolicy.DEFAULT;

	private boolean executionServiceEnabled = true;

	public Map<String, String> getProperties() {
		return properties;
	}

	public DebeziumFormat getFormat() {
		return format;
	}

	public void setFormat(DebeziumFormat format) {
		this.format = format;
	}

	public boolean isCopyHeaders() {
		return copyHeaders;
	}

	public void setCopyHeaders(boolean copyHeaders) {
		this.copyHeaders = copyHeaders;
	}

	public enum DebeziumOffsetCommitPolicy {
		/**
		 * Commits offsets as frequently as possible. This may result in reduced performance, but it has the least
		 * potential for seeing source records more than once upon restart.
		 */
		ALWAYS,
		/**
		 * Commits offsets no more than the specified time period. If the specified time is less than {@code 0} then the
		 * policy will behave as ALWAYS policy. Requires the 'spring.debezium.properties.offset.flush.interval.ms'
		 * native property to be set.
		 */
		PERIODIC,
		/**
		 * Uses the default Debezium engine policy (PERIODIC).
		 */
		DEFAULT;
	}

	public DebeziumOffsetCommitPolicy getOffsetCommitPolicy() {
		return offsetCommitPolicy;
	}

	public void setOffsetCommitPolicy(DebeziumOffsetCommitPolicy offsetCommitPolicy) {
		this.offsetCommitPolicy = offsetCommitPolicy;
	}

	/**
	 * Converts the Spring Framework "spring.debezium.properties.*" properties into native Debezium configuration.
	 */
	public Properties getDebeziumNativeConfiguration() {
		Properties outProps = new java.util.Properties();
		outProps.putAll(this.getProperties());
		return outProps;
	}

	public boolean isExecutionServiceEnabled() {
		return executionServiceEnabled;
	}

	public void setExecutionServiceEnabled(boolean executionServiceEnabled) {
		this.executionServiceEnabled = executionServiceEnabled;
	}

	public final String getContentTypeFor() {
		switch (this.format) {
			case JSON: return "application/json";
			case AVRO: return "application/avro";
			case PROTOBUF: return "application/x-protobuf";
			default: throw new IllegalStateException("Unknown Debezium format: " + format);
		}
	}
}
