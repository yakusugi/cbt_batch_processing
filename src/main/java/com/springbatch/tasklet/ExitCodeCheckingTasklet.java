package com.springbatch.tasklet;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class ExitCodeCheckingTasklet implements Tasklet {
    private final Tasklet delegate;

    public ExitCodeCheckingTasklet(Tasklet delegate) {
        this.delegate = delegate;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        int itemCount = contribution.getStepExecution().getWriteCount();

        if (itemCount == 0) {
            // If the result was zero, print "zero data" to the console and stop the process
            System.out.println("Zero data");
            contribution.setExitStatus(new ExitStatus("NO_DATA"));
            return RepeatStatus.FINISHED;
        }

        // If there is data, delegate the execution to the original tasklet
        return delegate.execute(contribution, chunkContext);
    }
}
