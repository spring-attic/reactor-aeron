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

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import reactor.core.Disposable;
import reactor.util.Logger;
import reactor.util.Loggers;

/** @author Anatoly Kadyshev */
public final class AeronWrapper implements Disposable {

  private static final Logger logger = Loggers.getLogger(AeronWrapper.class);

  private final String category;

  private static final DriverManager driverManager = new DriverManager();

  private final Aeron aeron;

  private final boolean isDriverLaunched;

  public AeronWrapper(String category, AeronOptions options) {
    this.category = category;
    if (options.getAeron() == null) {
      driverManager.launchDriver();
      aeron = driverManager.getAeron();
      isDriverLaunched = true;
    } else {
      aeron = options.getAeron();
      isDriverLaunched = false;
    }
  }

  @Override
  public void dispose() {
    if (isDriverLaunched) {
      driverManager.shutdownDriver().block();
    }
  }

  public Publication addPublication(String channel, int streamId, String purpose, long sessionId) {
    Publication publication = aeron.addPublication(channel, streamId);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "[{}] Added publication{} {} {}",
          category,
          formatSessionId(sessionId),
          purpose,
          AeronUtils.format(channel, streamId));
    }
    return publication;
  }

  public Subscription addSubscription(
      String channel, int streamId, String purpose, long sessionId) {
    Subscription subscription = aeron.addSubscription(channel, streamId);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "[{}] Added subscription{} {} {}",
          category,
          formatSessionId(sessionId),
          purpose,
          AeronUtils.format(channel, streamId));
    }
    return subscription;
  }

  private String formatSessionId(long sessionId) {
    return sessionId > 0 ? " sessionId: " + sessionId : "";
  }
}
