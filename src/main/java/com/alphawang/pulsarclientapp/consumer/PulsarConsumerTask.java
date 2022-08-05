package com.alphawang.pulsarclientapp.consumer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PulsarConsumerTask implements Runnable, Closeable {

  private PulsarClient pulsarClient;
  private Consumer<String> pulsarConsumer;
  private AtomicLong index = new AtomicLong();

  public PulsarConsumerTask() {
    try {
      this.pulsarClient = PulsarClient.builder()
          .serviceUrl("pulsar://localhost:6650")
          .build();
      this.pulsarConsumer = pulsarClient.newConsumer(Schema.STRING)
          .consumerName("AlphaTestConsumer")
          .topic("ALPHA.PULSAR.TEST")
          .subscriptionName("AlphaTestConsumer-subs")
          .subscriptionMode(SubscriptionMode.Durable)
          .subscriptionType(SubscriptionType.Failover)
          .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
          .subscribe();
    } catch (PulsarClientException e) {
      log.error("Failed to init puslar client", e);
    }
  }


  @Override
  public void run() {
    while (true) {
      try {
        Message<String> msg = pulsarConsumer.receive();
        log.info("--> received msg: id={}, payload={} ", msg.getMessageId(), msg.getValue());
      } catch (PulsarClientException e) {
        log.error("Failed to receive msg.", e);
      }

      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }


  @Override
  public void close() throws IOException {
    pulsarConsumer.close();
    pulsarClient.close();
  }
}
