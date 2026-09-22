package com.github.lisama569.hpcbatchschedulerjava.model;

/** 成组作业中的单个任务及其资源需求。 */
public record TaskSpec(String name, Resources requirement) {
}
