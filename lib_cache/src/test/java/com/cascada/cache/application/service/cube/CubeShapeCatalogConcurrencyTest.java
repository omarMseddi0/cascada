package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.domain.model.time.TimeRange;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CubeShapeCatalogConcurrencyTest {

    private static final TimeRange WINDOW = new TimeRange(0, 86_399);
    private static final QueryShape SHAPE = new QueryShape(Set.of(), Set.of(), Set.of("SUM(bytes)"));
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void expiresWindowsAtTheConfiguredTtlUsingTheCurrentClock() {
        MutableClock clock = new MutableClock(START);
        CubeShapeCatalog catalog = new CubeShapeCatalog(4, 4, Duration.ofMinutes(5), clock);
        catalog.register(WINDOW, SHAPE, frame(1), "revision-a", Optional.empty());

        assertThat(catalog.tryAnswer(WINDOW, SHAPE, "revision-a")).isPresent();

        clock.advance(Duration.ofMinutes(5));

        assertThat(catalog.windowCount()).isZero();
        assertThat(catalog.tryAnswer(WINDOW, SHAPE, "revision-a")).isEmpty();
    }

    @Test
    void dependencyTtlShortensAWindowAndLaterRegistrationsCannotExtendIt() {
        MutableClock clock = new MutableClock(START);
        CubeShapeCatalog catalog = new CubeShapeCatalog(4, 4, Duration.ofMinutes(5), clock);
        catalog.register(WINDOW, SHAPE, frame(1), "revision-a", Optional.of(Duration.ofSeconds(30)));

        clock.advance(Duration.ofSeconds(10));
        QueryShape anotherShape = new QueryShape(Set.of(), Set.of(), Set.of("COUNT(bytes)"));
        catalog.register(WINDOW, anotherShape, frame(2), "revision-a", Optional.of(Duration.ofMinutes(2)));
        assertThat(catalog.entryCount()).isEqualTo(2);

        clock.advance(Duration.ofSeconds(20));

        assertThat(catalog.windowCount()).isZero();
    }

    @Test
    void aNewRevisionReplacesTheWindowAndItsAnswers() {
        CubeShapeCatalog catalog = new CubeShapeCatalog(4, 4, Duration.ofMinutes(5), Clock.fixed(START, ZoneOffset.UTC));
        ResultFrame oldFrame = frame(1);
        ResultFrame newFrame = frame(2);
        catalog.register(WINDOW, SHAPE, oldFrame, "revision-old", Optional.empty());

        catalog.register(WINDOW, SHAPE, newFrame, "revision-new", Optional.empty());

        assertThat(catalog.tryAnswer(WINDOW, SHAPE, "revision-old")).isEmpty();
        assertThat(catalog.tryAnswer(WINDOW, SHAPE, "revision-new")).containsSame(newFrame);
        assertThat(catalog.entryCount()).isEqualTo(1);
    }

    @Test
    void simultaneousRegistrationsOfTheSameShapePublishOnlyOneCompleteFrame() throws Exception {
        int writers = 8;
        CubeShapeCatalog catalog = new CubeShapeCatalog(4, 4, Duration.ofMinutes(5), Clock.fixed(START, ZoneOffset.UTC));
        CyclicBarrier startTogether = new CyclicBarrier(writers);
        ExecutorService executor = Executors.newFixedThreadPool(writers);
        List<ResultFrame> frames = new ArrayList<>();
        List<Future<?>> writes = new ArrayList<>();
        try {
            for (int writer = 0; writer < writers; writer++) {
                ResultFrame candidate = frame(writer);
                frames.add(candidate);
                writes.add(executor.submit(() -> {
                    startTogether.await();
                    catalog.register(WINDOW, SHAPE, candidate, "revision-a", Optional.empty());
                    return null;
                }));
            }
            for (Future<?> write : writes) write.get();
        }
        finally {
            executor.shutdownNow();
        }

        assertThat(catalog.entryCount()).isEqualTo(1);
        ResultFrame published = catalog.tryAnswer(WINDOW, SHAPE, "revision-a").orElseThrow();
        assertThat(frames).contains(published);
        assertThat(published.rowCount()).isEqualTo(1);
        assertThat(published.columnNames()).containsExactly("SUM(bytes)");
        assertThat(((Number) published.valueAt(0, 0)).intValue()).isBetween(0, writers - 1);
    }

    @Test
    void registrationClearAndLookupCanRaceWithoutExposingPartialFrames() throws Exception {
        int iterations = 500;
        CubeShapeCatalog catalog = new CubeShapeCatalog(4, 4, Duration.ofMinutes(5), Clock.fixed(START, ZoneOffset.UTC));
        ResultFrame sharedFrame = frame(7);
        CyclicBarrier startTogether = new CyclicBarrier(3);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<?> registrations = executor.submit(() -> {
                startTogether.await();
                for (int index = 0; index < iterations; index++) {
                    catalog.register(WINDOW, SHAPE, sharedFrame, "revision-a", Optional.empty());
                }
                return null;
            });
            Future<?> flushes = executor.submit(() -> {
                startTogether.await();
                for (int index = 0; index < iterations; index++) catalog.clear();
                return null;
            });
            Future<?> lookups = executor.submit(() -> {
                startTogether.await();
                for (int index = 0; index < iterations; index++) {
                    Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, SHAPE, "revision-a");
                    if (answer.isPresent()) {
                        ResultFrame frame = answer.get();
                        assertThat(frame.rowCount()).isEqualTo(1);
                        assertThat(frame.columnNames()).containsExactly("SUM(bytes)");
                        assertThat(frame.valueAt(0, 0)).isEqualTo(7L);
                    }
                }
                return null;
            });
            registrations.get();
            flushes.get();
            lookups.get();
        }
        finally {
            executor.shutdownNow();
        }

        assertThat(catalog.windowCount()).isBetween(0, 1);
        assertThat(catalog.entryCount()).isBetween(0, 1);
        catalog.tryAnswer(WINDOW, SHAPE, "revision-a").ifPresent(answer -> assertThat(answer).isSameAs(sharedFrame));
    }

    @Test
    void clearDuringByteEstimationPreventsTheEarlierRegistrationFromReappearing() throws Exception {
        CountDownLatch estimationEntered = new CountDownLatch(1);
        CountDownLatch finishEstimation = new CountDownLatch(1);
        BigDecimal gatedDecimal = new GatedBigDecimal(estimationEntered, finishEstimation);
        ResultFrame frame = ResultFrame.builder()
                .column("SUM(bytes)", ColumnType.DECIMAL)
                .row(gatedDecimal)
                .build();
        CubeShapeCatalog catalog = new CubeShapeCatalog(4, 4, Duration.ofMinutes(5), Clock.fixed(START, ZoneOffset.UTC));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> registration = executor.submit(() -> {
                catalog.register(WINDOW, SHAPE, frame, "revision-a", Optional.empty());
                return null;
            });

            assertThat(estimationEntered.await(30, TimeUnit.SECONDS)).isTrue();
            CountDownLatch clearCompleted = new CountDownLatch(1);
            Future<?> clear = executor.submit(() -> {
                catalog.clear();
                clearCompleted.countDown();
                return null;
            });
            boolean clearFinishedWhileEstimationWasBlocked = clearCompleted.await(30, TimeUnit.SECONDS);
            finishEstimation.countDown();
            clear.get();
            registration.get();

            assertThat(clearFinishedWhileEstimationWasBlocked).isTrue();
            assertThat(catalog.windowCount()).isZero();
            assertThat(catalog.tryAnswer(WINDOW, SHAPE, "revision-a")).isEmpty();
        }
        finally {
            finishEstimation.countDown();
            executor.shutdownNow();
        }
    }


    @Test
    void estimationTimeCannotExtendDependencyLifetime() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        MutableClock clock = new MutableClock(START);
        CubeShapeCatalog catalog = new CubeShapeCatalog(4, 4, Duration.ofMinutes(5), clock);
        ResultFrame value = ResultFrame.builder().column("SUM(bytes)", ColumnType.DECIMAL)
                .row(new GatedBigDecimal(entered, release)).build();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> registration = worker.submit(() -> catalog.register(WINDOW, SHAPE, value,
                    "revision-a", Optional.of(Duration.ofSeconds(10))));
            assertThat(entered.await(30, TimeUnit.SECONDS)).isTrue();
            clock.advance(Duration.ofSeconds(11));
            release.countDown();
            registration.get(30, TimeUnit.SECONDS);
            assertThat(catalog.tryAnswer(WINDOW, SHAPE, "revision-a")).isEmpty();
            assertThat(catalog.windowCount()).isZero();
        } finally { release.countDown(); worker.shutdownNow(); }
    }

    @Test
    void slowerEarlierRegistrationCannotReplaceARevisionPublishedDuringItsScan() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        CubeShapeCatalog catalog = new CubeShapeCatalog();
        ResultFrame older = ResultFrame.builder().column("SUM(bytes)", ColumnType.DECIMAL)
                .row(new GatedBigDecimal(entered, release)).build();
        ResultFrame newer = frame(9L);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> oldRegistration = workers.submit(() -> catalog.register(WINDOW, SHAPE, older,
                    "revision-old", Optional.empty()));
            assertThat(entered.await(30, TimeUnit.SECONDS)).isTrue();
            workers.submit(() -> catalog.register(WINDOW, SHAPE, newer, "revision-new", Optional.empty()))
                    .get(30, TimeUnit.SECONDS);
            release.countDown();
            oldRegistration.get(30, TimeUnit.SECONDS);
            assertThat(catalog.tryAnswer(WINDOW, SHAPE, "revision-new")).containsSame(newer);
            assertThat(catalog.tryAnswer(WINDOW, SHAPE, "revision-old")).isEmpty();
        } finally { release.countDown(); workers.shutdownNow(); }
    }

    private static ResultFrame frame(long writer) {
        return ResultFrame.builder()
                .column("SUM(bytes)", ColumnType.LONG)
                .row(writer)
                .build();
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> current;

        private MutableClock(Instant start) {
            current = new AtomicReference<>(start);
        }

        private void advance(Duration duration) {
            current.updateAndGet(instant -> instant.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return zone.equals(ZoneOffset.UTC) ? this : Clock.fixed(instant(), zone);
        }

        @Override
        public Instant instant() {
            return current.get();
        }
    }

    private static final class GatedBigDecimal extends BigDecimal {
        private final CountDownLatch estimationEntered;
        private final CountDownLatch finishEstimation;

        private GatedBigDecimal(CountDownLatch estimationEntered, CountDownLatch finishEstimation) {
            super("1.25");
            this.estimationEntered = estimationEntered;
            this.finishEstimation = finishEstimation;
        }

        @Override
        public int precision() {
            estimationEntered.countDown();
            try {
                finishEstimation.await();
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while gating byte estimation", interrupted);
            }
            return super.precision();
        }
    }
}
