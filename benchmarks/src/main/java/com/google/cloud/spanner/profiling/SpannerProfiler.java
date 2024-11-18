/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.profiling;

import com.google.api.core.ApiFuture;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Stopwatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.threeten.bp.Duration;

public class SpannerProfiler {

  public static void main(String[] args) throws InterruptedException {
    Spanner spanner =
        SpannerOptions.newBuilder()
            .setProjectId("span-cloud-testing")
            .setAsyncExecutorProvider(
                SpannerOptions.FixedCloseableExecutorProvider.create(
                    Executors.newScheduledThreadPool(3)))
            .setSessionPoolOption(
                SessionPoolOptions.newBuilder()
                    .setWaitForMinSessions(Duration.ofSeconds(20))
                    .build())
            .build()
            .getService();
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(
            DatabaseId.of("span-cloud-testing", "sakthi-spanner-testing", "testing-database"));

    Stopwatch stopwatch = Stopwatch.createStarted();
    triggerQuery(databaseClient, "SELECT ID,NAME FROM Employees LIMIT 100");
    System.out.println("Total time spent " + stopwatch.elapsed().toMillis());
    TimeUnit.SECONDS.sleep(30);
    spanner.close();
  }

  public static void triggerQuery(DatabaseClient databaseClient, String query) {
    try (ReadContext readContext = databaseClient.singleUse()) {
      ResultSet resultSet = readContext.executeQueryAsync(Statement.of(query));
      while (resultSet.next()) {
        System.out.println(resultSet.getCurrentRowAsStruct());
      }
    }
  }
}
