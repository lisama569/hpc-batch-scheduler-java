package com.github.lisama569.hpcbatchschedulerjava.web;

import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 暴露进程内调度核心为单例 Bean。 */
@Configuration
public class SchedulerConfiguration {

    @Bean
    public SchedulerService schedulerService() {
        return new SchedulerService();
    }
}
