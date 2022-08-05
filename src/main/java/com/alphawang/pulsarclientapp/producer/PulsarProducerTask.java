package com.alphawang.pulsarclientapp.producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PulsarProducerTask implements Runnable, Closeable {

  private PulsarClient pulsarClient;
  private Producer<String> pulsarProducer;
  private AtomicLong index = new AtomicLong();

  public PulsarProducerTask() {
    try {
      this.pulsarClient = PulsarClient.builder()
          .serviceUrl("pulsar://localhost:6650")
          .build();
      this.pulsarProducer = pulsarClient.newProducer(Schema.STRING)
          .producerName("AlphaTestProducer")
          .topic("ALPHA.PULSAR.TEST")
          .blockIfQueueFull(true)
          .batchingMaxBytes(1*1024*1024) // max batch 1M.
          .accessMode(ProducerAccessMode.Shared)
          .create();
    } catch (PulsarClientException e) {
      log.error("Failed to init puslar client", e);
    }
  }


  @Override
  public void run() {
    while (true) {
      try {
        MessageId msg = pulsarProducer.send("mock payload " + index.incrementAndGet());
        log.info("--> sent msg: " + msg);
      } catch (PulsarClientException e) {
        log.error("Failed to send msg.", e);
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
    pulsarProducer.close();
    pulsarClient.close();
  }
}
