/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.connection.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.MutationGroup;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.helpers.annotations.RepeatTest;
import com.google.cloud.spanner.helpers.annotations.RepeatedTestRule;
import com.google.common.base.Stopwatch;
import com.google.rpc.Code;
import com.google.spanner.v1.BatchWriteResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.threeten.bp.Duration;

@RunWith(Parameterized.class)
public class ITAsyncQueryTest {

  private static Spanner spanner;
  private static ExecutorService executor;
  private static DatabaseClient databaseClient;
  private static final String[] DEPARTMENTS = new String[]{"PHYSICS", "COMPUTER_SCIENCE", "CHEMISTRY", "MATHS"};

  @Rule
  public RepeatedTestRule repeatedTestRule = new RepeatedTestRule();

  @Parameterized.Parameter(0)
  public int limit;

  @Parameterized.Parameter(1)
  public int offset;

  @Parameterized.Parameter(2)
  public int option1;

  @Parameterized.Parameter(3)
  public int option2;

  @Parameterized.Parameter(4)
  public int option3;

  @Parameterized.Parameter(5)
  public int option4;

  @Parameterized.Parameter(6)
  public int option5;


  @Parameterized.Parameters(name = "rows = {6}")
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    params.add(new Object[]{100, 150, 90000, 110000, 220000, 330000, 1});
    params.add(new Object[]{150, 200, 80000, 120000, 230000, 430000, 2});
    params.add(new Object[]{200, 250, 70000, 130000, 240000, 530000, 3});
    params.add(new Object[]{250, 300, 60000, 140000, 250000, 630000, 4});
    params.add(new Object[]{300, 350, 50000, 150000, 260000, 730000, 5});
    return params;
  }

  @Before
  public void setUp() {
    spanner = SpannerOptions.newBuilder()
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
    executor = Executors.newFixedThreadPool(10);
    databaseClient = spanner.getDatabaseClient(DatabaseId.of("span-cloud-testing", "sakthi-spanner-testing", "testing-database"));
  }

  //    @Test
  public void setUpAndPopulateData() {
    int base = 1210000;
    List<MutationGroup> mutationGroups = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      List<Mutation> mutations = new ArrayList<>();
      for (int j = 0; j < 100; j++) {
        int id = base + (i * 100) + j;
        mutations.add(Mutation.newInsertOrUpdateBuilder("EMPLOYEES")
            .set("ID")
            .to(id)
            .set("NAME")
            .to(String.format("SAKTHI %s", id))
            .set("ABOUT")
            .to(generateRandomString())
            .set("DEPARTMENT")
            .to(getRandomDepartment())
            .set("CREATED_AT")
            .to(Timestamp.now())
            .build());
      }
      mutationGroups.add(MutationGroup.of(mutations));
    }
    ServerStream<BatchWriteResponse> responses = databaseClient.batchWriteAtLeastOnce(mutationGroups, Options.tag("batch-write-tag-1"));
    for (BatchWriteResponse response : responses) {
      if (response.getStatus().getCode() == Code.OK_VALUE) {
        System.out.printf(
            "Mutation group indexes %s have been applied with commit timestamp %s",
            response.getIndexesList(), response.getCommitTimestamp());
      } else {
        System.out.printf(
            "Mutation group indexes %s could not be applied with error code %s and "
                + "error message %s", response.getIndexesList(),
            Code.forNumber(response.getStatus().getCode()), response.getStatus().getMessage());
      }
    }
  }

  private String generateRandomString() {
    Random random = new Random();
    int max = random.nextInt( 399);
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < max; i++) {
      int rand = random.nextInt( 26);
      sb.append((char) ('A' + rand));
    }
    return sb.toString();
  }

  private String getRandomDepartment() {
    Random random = new Random();
    int max = random.nextInt(3);
    return DEPARTMENTS[max];
  }

  @After
  public void close() {
    spanner.close();
  }

  @RepeatTest(times = 10)
  @Test
  public void testQueryAsync() throws ExecutionException, InterruptedException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    final AtomicInteger rowCount = new AtomicInteger();
    List<ApiFuture<Void>> futures = new ArrayList<>();
    for(int i = 0; i < 10; i++) {
      ApiFuture<Void> r1 = triggerQuery(rowCount, String.format("SELECT * FROM Employees LIMIT %s", limit));
      ApiFuture<Void> r2 = triggerQuery(rowCount, String.format("SELECT * FROM Employees ORDER BY ID DESC LIMIT %s", limit));
      ApiFuture<Void> r3 = triggerQuery(rowCount, String.format("SELECT * FROM Employees WHERE ID > %s LIMIT %s", option1, limit));
      ApiFuture<Void> r4 = triggerQuery(rowCount, String.format("SELECT * FROM Employees WHERE ID < %s ORDER BY ID DESC LIMIT %s", option1, limit));
      ApiFuture<Void> r5 = triggerQuery(rowCount, String.format("SELECT * FROM Employees WHERE ID > %s LIMIT %s OFFSET %s", option1, limit, offset));
      ApiFuture<Void> r6 = triggerQuery(rowCount, String.format("SELECT * FROM Employees WHERE ID < %s ORDER BY ID DESC LIMIT %s", option2, limit));
      ApiFuture<Void> r7 = triggerQuery(rowCount, String.format("SELECT * FROM Employees WHERE ID > %s LIMIT %s OFFSET %s", option2, limit, offset));
      ApiFuture<Void> r8 = triggerQuery(rowCount, String.format("SELECT * FROM Employees WHERE ID < %s ORDER BY ID DESC LIMIT %s", option3, limit));
      ApiFuture<Void> r9 = triggerQuery(rowCount, String.format("SELECT * FROM Employees WHERE ID > %s LIMIT %s OFFSET %s", option3, limit, offset));
      ApiFuture<Void> r10 = triggerQuery(rowCount, String.format("SELECT * FROM Employees WHERE ID < %s ORDER BY ID DESC LIMIT %s", option4, limit));

      futures.add(r1);
      futures.add(r2);
      futures.add(r3);
      futures.add(r4);
      futures.add(r5);
      futures.add(r6);
      futures.add(r7);
      futures.add(r8);
      futures.add(r9);
      futures.add(r10);
    }
    ApiFutures.allAsList(futures).get();
    System.out.println(stopwatch.stop().elapsed().toMillis());
    assertThat(rowCount.get()).isNotEqualTo(0);

  }

  public ApiFuture<Void> triggerQuery(AtomicInteger rowCount, String query) {
    AsyncResultSet asyncResultSet;
    ApiFuture<Void> res;
    try (ReadContext readContext = databaseClient.singleUse()) {
      asyncResultSet = readContext.executeQueryAsync(Statement.of(query));
      res = asyncResultSet.setCallback(executor,
          resultSet -> {
            while (true) {
              switch (resultSet.tryNext()) {
                case OK:
//                                    System.out.println(resultSet.getCurrentRowAsStruct());
                  rowCount.incrementAndGet();
                  break;
                case DONE:
                  return AsyncResultSet.CallbackResponse.DONE;
                case NOT_READY:
                  return AsyncResultSet.CallbackResponse.CONTINUE;
              }
            }
          });
    }
    return res;
  }

}