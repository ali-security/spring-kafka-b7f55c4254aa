/*
 * Copyright 2026 the original author or authors.
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

package org.springframework.kafka.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.DefaultKafkaHeaderMapper.NonTrustedHeaderType;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Tests for trusting packages in {@link DefaultKafkaHeaderMapper}.
 *
 * @author Soby Chacko
 */
public class DefaultKafkaHeaderMapperTrustPackagesTests {

	@Test
	void subpackagesOfDefaultsAreNotTrustedTransitively() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		assertThat(mapper.trusted("java.util.logging.FileHandler")).isFalse();
		assertThat(mapper.trusted("java.lang.reflect.Method")).isFalse();
		assertThat(mapper.trusted("java.util.concurrent.ForkJoinPool")).isFalse();
	}

	@Test
	void exactMatchAgainstDefaultsStillWorks() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		assertThat(mapper.trusted("java.util.HashMapr")).isTrue();
		assertThat(mapper.trusted("java.lang.String")).isTrue();
		assertThat(mapper.trusted("java.util.UUID")).isTrue();
	}

	@Test
	void userAddedSubpackagesMustBeAddedExplicitly() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		mapper.addTrustedPackages("com.acme");
		assertThat(mapper.trusted("com.acme.OrderEvent")).isTrue();
		assertThat(mapper.trusted("com.acme.events.OrderEvent")).isFalse();
		mapper.addTrustedPackages("com.acme.events");
		assertThat(mapper.trusted("com.acme.events.OrderEvent")).isTrue();
	}

	@Test
	void inetAddressTypesAreNeverTrusted() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		assertThat(mapper.trusted("java.net.InetAddress")).isFalse();
		assertThat(mapper.trusted("java.net.Inet4Address")).isFalse();
		assertThat(mapper.trusted("java.net.Inet6Address")).isFalse();
		assertThat(mapper.trusted("java.net.InetSocketAddress")).isFalse();
		assertThat(mapper.trusted("[Ljava.net.InetAddress;")).isFalse();
		assertThat(mapper.trusted("java.net.URI")).isTrue();
		mapper.addTrustedPackages("*");
		assertThat(mapper.trusted("java.net.InetAddress")).isFalse();
		assertThat(mapper.trusted("[Ljava.net.InetSocketAddress;")).isFalse();
		assertThat(mapper.trusted("java.net.URI")).isTrue();
	}

	@Test
	void inetAddressHeaderIsReturnedAsNonTrusted() throws Exception {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		Message<String> message = MessageBuilder.withPayload("foo")
				.setHeader("addr", InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 }))
				.setHeader("uri", URI.create("https://foo.bar"))
				.build();
		RecordHeaders recordHeaders = new RecordHeaders();
		mapper.fromHeaders(message.getHeaders(), recordHeaders);
		Map<String, Object> headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("addr")).isInstanceOf(NonTrustedHeaderType.class);
		assertThat(((NonTrustedHeaderType) headers.get("addr")).getUntrustedType())
				.isEqualTo("java.net.Inet4Address");
		assertThat(headers.get("uri")).isEqualTo(URI.create("https://foo.bar"));
	}

}
