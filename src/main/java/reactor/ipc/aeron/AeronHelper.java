/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.ipc.aeron;

import reactor.util.Logger;
import reactor.util.Loggers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;

import java.util.UUID;

/**
 * @author Anatoly Kadyshev
 */
public final class AeronHelper {

    private final Logger logger;

    private final static DriverManager driverManager = new DriverManager();

    private final Aeron aeron;

    private final boolean isDriverLaunched;

    public AeronHelper(String category, AeronOptions options) {
        this.logger = Loggers.getLogger(AeronHelper.class + "." + category);

        if (options.getAeron() == null) {
            driverManager.launchDriver();
            aeron = driverManager.getAeron();
            isDriverLaunched = true;
        } else {
            aeron = options.getAeron();
            isDriverLaunched = false;
        }
    }

    public void shutdown() {
        if (isDriverLaunched) {
            driverManager.shutdownDriver().block();
        }
    }

    public Publication addPublication(String channel, int streamId, String purpose, UUID sessionId) {
        Publication publication = aeron.addPublication(channel, streamId);
        logger.debug("Added publication, sessionId: {} for {}: {}/{}", sessionId, purpose, channel, streamId);
        return publication;
    }

    public Subscription addSubscription(String channel, int streamId, String purpose, UUID sessionId) {
        Subscription subscription = aeron.addSubscription(channel, streamId);

        logger.debug("Added subscription{} for {}: {}/{}", sessionId != null ? ", sessionId: " + sessionId: "",
                purpose, channel, streamId);
        return subscription;
    }

}
