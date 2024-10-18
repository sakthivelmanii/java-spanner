package com.google.cloud.spanner.connection.it;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.spanner.*;
import com.google.rpc.Code;
import com.google.spanner.v1.BatchWriteResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class ITAsyncQueryTest {

    private static Spanner spanner;
    private static ExecutorService executor;
    private static DatabaseClient databaseClient;

    @Before
    public void setUp() {
        spanner = SpannerOptions.newBuilder()
                .setProjectId("span-cloud-testing")
                .setAsyncExecutorProvider(SpannerOptions.FixedCloseableExecutorProvider.create(Executors.newScheduledThreadPool(3)))
                .setSessionPoolOption(SessionPoolOptions.newBuilder().setFailOnSessionLeak().build())
                .build()
                .getService();
        executor = Executors.newFixedThreadPool(10);
        databaseClient = spanner.getDatabaseClient(DatabaseId.of("span-cloud-testing", "sakthi-spanner-testing", "testing-database"));
    }

    @Test
    public void setUpAndPopulateData() {
//        databaseClient.
        int base = 200000;
        List<MutationGroup> mutationGroups = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            List<Mutation> mutations = new ArrayList<>();
            for (int j = 0; j < 100; j++) {
                int id = base + (i * 100) + j;
//                System.out.println(id);
                mutations.add(Mutation.newInsertOrUpdateBuilder("TEST")
                        .set("ID")
                        .to(id)
                        .set("NAME")
                        .to(String.format("SAKTHI %s", id))
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

    @After
    public void close() {
        spanner.close();
    }

    @Test
    public void testQueryAsync() throws ExecutionException, InterruptedException {
        final AtomicInteger rowCount = new AtomicInteger();
        ApiFuture<Void> r1 = triggerQuery(rowCount, "1");
        ApiFuture<Void> r2 = triggerQuery(rowCount, "2");
        ApiFuture<Void> r3 = triggerQuery(rowCount, "3");
        ApiFuture<Void> r4 = triggerQuery(rowCount, "4");

        List<ApiFuture<Void>> futures = new ArrayList<>();
        futures.add(r1);
        futures.add(r2);
        futures.add(r3);
        futures.add(r4);
        ApiFutures.allAsList(futures).get();
        assertThat(rowCount.get()).isEqualTo(300000);

    }

    public ApiFuture<Void> triggerQuery(AtomicInteger rowCount, String version) {
        AsyncResultSet asyncResultSet;
        ApiFuture<Void> res;
        try (ReadContext readContext = databaseClient.singleUse()) {
            asyncResultSet = readContext.executeQueryAsync(Statement.of("SELECT * FROM TEST"), Options.bufferRows(20));
            res = asyncResultSet.setCallback(executor,
                    resultSet -> {
//                        System.out.println(resultSet);
                        while (true) {
                            switch (resultSet.tryNext()) {
                                case OK:
                                    System.out.println(version);
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
