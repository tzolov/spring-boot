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

import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.debezium.DebeziumProperties.DebeziumFormat;
import org.springframework.boot.autoconfigure.debezium.DebeziumProperties.DebeziumOffsetCommitPolicy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link DebeziumProperties}.
 *
 * @author Christian Tzolov
 */
class DebeziumPropertiesTests {

    DebeziumProperties properties = new DebeziumProperties();

    @Test
    public void defaultPropertiesTest() {
        assertThat(this.properties.getFormat()).isEqualTo(DebeziumFormat.JSON);
        assertThat(this.properties.isCopyHeaders()).isEqualTo(true);
        assertThat(this.properties.getOffsetCommitPolicy()).isEqualTo(DebeziumOffsetCommitPolicy.DEFAULT);
        assertThat(this.properties.isExecutionServiceEnabled()).isEqualTo(true);
        assertThat(this.properties.getProperties()).isNotNull();
        assertThat(this.properties.getProperties()).isEmpty();
    }

    @Test
    public void debeziumFormatTest() {
        this.properties.setFormat(DebeziumFormat.AVRO);
        assertThat(this.properties.getFormat()).isEqualTo(DebeziumFormat.AVRO);
        assertThat(this.properties.getFormat().serializationFormat())
                .isEqualTo(io.debezium.engine.format.Avro.class);
        assertThat(this.properties.getFormat().contentType()).isEqualTo("application/avro");

        this.properties.setFormat(DebeziumFormat.JSON);
        assertThat(this.properties.getFormat()).isEqualTo(DebeziumFormat.JSON);
        assertThat(this.properties.getFormat().serializationFormat())
                .isEqualTo(io.debezium.engine.format.JsonByteArray.class);
        assertThat(this.properties.getFormat().contentType()).isEqualTo("application/json");

        this.properties.setFormat(DebeziumFormat.PROTOBUF);
        assertThat(this.properties.getFormat()).isEqualTo(DebeziumFormat.PROTOBUF);
        assertThat(this.properties.getFormat().serializationFormat())
                .isEqualTo(io.debezium.engine.format.Protobuf.class);
        assertThat(this.properties.getFormat().contentType()).isEqualTo("application/x-protobuf");
    }

}
