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

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation.Builder;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.impl.auth.v5.HttpChallengeTransport;

/**
 * Executes the SASL HTTP challenge over the owning admin's configured Jersey
 * client, including its TLS trust, provider, and connector configuration.
 */
final class AdminHttpChallengeTransport implements HttpChallengeTransport {

    private final Client client;

    AdminHttpChallengeTransport(Client client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<Result> get(URI uri, HttpAuthHeaders requestHeaders, Duration timeout) {
        CompletableFuture<Result> result = new CompletableFuture<>();
        try {
            Builder builder = client.target(uri).request(MediaType.APPLICATION_JSON);
            requestHeaders.asMap().forEach(builder::header);
            Future<Response> request = builder.async().get(new InvocationCallback<Response>() {
                @Override
                public void completed(Response response) {
                    try {
                        Map<String, String> headers = new LinkedHashMap<>();
                        response.getStringHeaders().forEach((name, values) -> {
                            if (values != null && !values.isEmpty()) {
                                headers.put(name, values.get(0));
                            }
                        });
                        result.complete(new Result(response.getStatus(), HttpAuthHeaders.of(headers)));
                    } catch (Throwable t) {
                        result.completeExceptionally(t);
                    } finally {
                        response.close();
                    }
                }

                @Override
                public void failed(Throwable error) {
                    result.completeExceptionally(error);
                }
            });
            if (timeout != null && !timeout.isNegative() && !timeout.isZero()) {
                result.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
            }
            result.whenComplete((value, error) -> {
                if (error != null && !request.isDone()) {
                    request.cancel(false);
                }
            });
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
        return result;
    }
}
