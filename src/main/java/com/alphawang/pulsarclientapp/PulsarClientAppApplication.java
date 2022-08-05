package com.alphawang.pulsarclientapp;

import com.alphawang.pulsarclientapp.consumer.PulsarConsumerTask;
import com.alphawang.pulsarclientapp.producer.PulsarProducerTask;
import java.util.concurrent.Executor;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@EnableScheduling
@SpringBootApplication
public class PulsarClientAppApplication {

  @Autowired
  private PulsarProducerTask producerTask;

  @Autowired
  private PulsarConsumerTask consumerTask;

  @Autowired
  private Executor asyncServiceExecutor;

  public static void main(String[] args) {
    SpringApplication.run(PulsarClientAppApplication.class, args);
  }

  @PostConstruct
  public void start() {
    asyncServiceExecutor.execute(producerTask);
//    asyncServiceExecutor.execute(consumerTask);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        producerTask.close();
        consumerTask.close();
      } catch (Exception e) {
        log.error("Failed to close", e);
      }

    }));
  }

}