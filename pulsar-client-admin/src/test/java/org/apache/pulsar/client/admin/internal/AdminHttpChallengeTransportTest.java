/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.admin.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.client.AsyncInvoker;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation.Builder;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.testng.annotations.Test;

public class AdminHttpChallengeTransportTest {

    @Test
    public void challengeUsesSuppliedAdminClientAndClosesResponse() throws Exception {
        URI uri = URI.create("https://admin.example:8443/admin/v2/clusters");
        Client configuredClient = mock(Client.class);
        WebTarget target = mock(WebTarget.class);
        Builder builder = mock(Builder.class);
        AsyncInvoker asyncInvoker = mock(AsyncInvoker.class);
        Response response = mock(Response.class);
        AtomicReference<InvocationCallback<Response>> callback = new AtomicReference<>();
        CompletableFuture<Response> underlying = new CompletableFuture<>();

        when(configuredClient.target(uri)).thenReturn(target);
        when(target.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
        when(builder.async()).thenReturn(asyncInvoker);
        when(asyncInvoker.get(any(InvocationCallback.class))).thenAnswer(invocation -> {
            callback.set(invocation.getArgument(0));
            return underlying;
        });
        when(response.getStatus()).thenReturn(401);
        MultivaluedHashMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("Sasl-Challenge", "next");
        when(response.getStringHeaders()).thenReturn(headers);

        CompletableFuture<org.apache.pulsar.client.impl.auth.v5.HttpChallengeTransport.Result> result =
                new AdminHttpChallengeTransport(configuredClient).get(
                        uri, HttpAuthHeaders.of(Map.of("Sasl-Request", "first")), Duration.ofSeconds(10));
        underlying.complete(response);
        callback.get().completed(response);

        assertThat(result.get(1, TimeUnit.SECONDS).statusCode()).isEqualTo(401);
        assertThat(result.get().responseHeaders().asMap()).containsEntry("Sasl-Challenge", "next");
        verify(configuredClient).target(uri);
        verify(builder).header("Sasl-Request", "first");
        verify(response).close();
    }

    @Test
    public void connectionFailureIsReportedOnFuture() {
        URI uri = URI.create("https://admin.example:8443/admin/v2/clusters");
        Client configuredClient = mock(Client.class);
        when(configuredClient.target(uri)).thenThrow(new IllegalStateException("TLS connection unavailable"));

        CompletableFuture<?> result = new AdminHttpChallengeTransport(configuredClient).get(
                uri, HttpAuthHeaders.empty(), Duration.ofSeconds(1));

        assertThatThrownBy(result::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TLS connection unavailable");
    }
}
