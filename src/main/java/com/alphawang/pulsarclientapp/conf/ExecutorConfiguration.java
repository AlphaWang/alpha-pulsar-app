package com.alphawang.pulsarclientapp.conf;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Slf4j
@EnableAsync
@Configuration
public class ExecutorConfiguration {

  @Bean(name = "asyncServiceExecutor")
  public Executor asyncServiceExecutor() {
    log.info("start asyncServiceExecutor");
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    //Configure the number of core threads
    executor.setCorePoolSize(2);
    //Configure maximum threads
    executor.setMaxPoolSize(4);
    //Configure queue size
    executor.setQueueCapacity(100);
    //Configure the name prefix of threads in the thread pool
    executor.setThreadNamePrefix("pulsar-task");

    // Rejection policy: how to handle new tasks when the pool has reached max size
    // CALLER_RUNS: the task is not executed in the new thread, but in the thread of the caller
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    //Perform initialization
    executor.initialize();
    return executor;
  }

}
