/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.aeron.publisher;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.Context;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.aeron.utils.AeronTestUtils;
import reactor.aeron.utils.SignalPublicationFailedException;
import reactor.aeron.utils.ThreadSnapshot;
import reactor.core.publisher.Flux;
import reactor.test.subscriber.AssertSubscriber;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * @author Anatoly Kadyshev
 */
public abstract class CommonSubscriberPublisherTest {

	public static final Duration TIMEOUT = Duration.ofSeconds(5);

	final String senderChannel = AeronTestUtils.availableLocalhostChannel();

	private ThreadSnapshot threadSnapshot;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();
	}

	@After
	public void doTearDown() throws InterruptedException {
		AeronTestUtils.assertThreadsTerminated(threadSnapshot);
	}

	protected abstract Context createContext(String name);

	@Test
	public void testNextSignalIsReceivedByPublisher() throws InterruptedException {
		AeronSubscriber subscriber = AeronSubscriber.create(createContext("subscriber"));

        AeronTestUtils.newByteBufferFlux("One", "Two", "Three")
		    .subscribe(subscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher"));

		AssertSubscriber<String> clientSubscriber = AssertSubscriber.create();
		AeronTestUtils.bufferToString(publisher).subscribe(clientSubscriber);


		clientSubscriber.awaitAndAssertNextValues("One", "Two", "Three").assertComplete();
	}

	@Test
	public void testErrorShutsDownSenderAndReceiver() throws InterruptedException {
		AeronSubscriber subscriber = AeronSubscriber.create(createContext("subscriber"));
		AeronFlux publisher = new AeronFlux(createContext("publisher"));

		AssertSubscriber<String> clientSubscriber = AssertSubscriber.create();
		AeronTestUtils.bufferToString(publisher).subscribe(clientSubscriber);


		Flux.<ByteBuffer>error(new RuntimeException("Something went wrong")).subscribe(subscriber);

		clientSubscriber.await().assertError();
	}

	@Test
	public void testFailedOnNextSignalPublicationIsReported() throws InterruptedException {
		final CountDownLatch gotErrorLatch = new CountDownLatch(1);
		final AtomicReference<Throwable> error = new AtomicReference<>();
		AeronSubscriber subscriber = AeronSubscriber.create(createContext("subscriber").errorConsumer(th -> {
			gotErrorLatch.countDown();
			error.set(th);
		}));

		final byte[] bytes = new byte[2048];
		Flux.range(1, 100).map(i -> ByteBuffer.wrap(bytes)).subscribe(subscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher").autoCancel(false));

		CountDownLatch onNextLatch = new CountDownLatch(1);
		publisher.subscribe(buffer -> {
				try {
					onNextLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
		});

		assertTrue(gotErrorLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
		onNextLatch.countDown();

		assertThat(error.get(), Matchers.instanceOf(SignalPublicationFailedException.class));
	}

	static class TestPublisher implements Publisher<ByteBuffer> {

		private Subscriber<? super ByteBuffer> subscriber;

		private final CountDownLatch cancelledLatch = new CountDownLatch(1);

		@Override
		public void subscribe(Subscriber<? super ByteBuffer> s) {
			subscriber = s;
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					subscriber.onNext(AeronTestUtils.stringToByteBuffer(String.valueOf(n)));
				}

				@Override
				public void cancel() {
					cancelledLatch.countDown();
				}
			});
		}

		public boolean awaitCancelled(Duration timeout) throws InterruptedException {
			return cancelledLatch.await(timeout.getSeconds(), TimeUnit.SECONDS);
		}

	}

	@Test
	public void testUpstreamSubscriptionIsCancelledWhenAutoCancel() throws InterruptedException {
		TestPublisher valuePublisher = new TestPublisher();

		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber").autoCancel(true));
		valuePublisher.subscribe(aeronSubscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher").autoCancel(true));
		AssertSubscriber<String> client = AssertSubscriber.create(0);
		AeronTestUtils.bufferToString(publisher).subscribe(client);

		client.request(1);
		client.awaitAndAssertNextValueCount(1);
		client.cancel();


		assertTrue(valuePublisher.awaitCancelled(TIMEOUT));
	}

	@Test
	public void testUpstreamSubscriptionIsNotCancelledWhenNoAutoCancel() throws InterruptedException {
		TestPublisher valuePublisher = new TestPublisher();

		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber").autoCancel(false));
		valuePublisher.subscribe(aeronSubscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher").autoCancel(false));
		AssertSubscriber<String> client = AssertSubscriber.create(0);
		AeronTestUtils.bufferToString(publisher).subscribe(client);

		client.request(1);
		client.awaitAndAssertNextValueCount(1);
		client.cancel();


		assertFalse(valuePublisher.awaitCancelled(Duration.ofSeconds(2)));

		publisher.shutdown();
		aeronSubscriber.shutdown();
	}

}
