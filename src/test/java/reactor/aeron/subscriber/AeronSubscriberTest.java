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
package reactor.aeron.subscriber;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.aeron.Context;
import reactor.aeron.utils.AeronTestUtils;
import reactor.aeron.utils.ThreadSnapshot;
import reactor.core.publisher.Flux;
import reactor.core.publisher.TopicProcessor;
import reactor.test.subscriber.AssertSubscriber;

/**
 * @author Anatoly Kadyshev
 */
public class AeronSubscriberTest {

	private ThreadSnapshot threadSnapshot;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();
	}

	@After
	public void doTearDown() throws InterruptedException {
		AeronTestUtils.awaitMediaDriverIsTerminated();

		AeronTestUtils.assertThreadsTerminated(threadSnapshot);
	}

	@Test
	public void testShutdown() {
		AeronSubscriber subscriber = AeronSubscriber.create(Context.create()
				.name("publisher")
				.senderChannel(AeronTestUtils.availableLocalhostChannel()));

		subscriber.shutdown();
	}

	@Test
	public void testTopicProcessor() {
        TopicProcessor<String> processor = TopicProcessor.create();
        Flux.just("1", "2", "3").subscribe(processor);

        AssertSubscriber<String> subscriber = AssertSubscriber.create(0);
        processor.subscribe(subscriber);

        subscriber.request(3);

        subscriber.awaitAndAssertNextValues("1", "2", "3");

        //FIXME: This call is redundant and should be removed
        subscriber.request(1);
        subscriber.await(AeronTestUtils.TIMEOUT).assertComplete();
    }

}