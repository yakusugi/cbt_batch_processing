package com.springbatch.tasklet;

import com.springbatch.utility.FileUtil;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
@Component
public class LatestFileTasklet implements Tasklet {

    String latestFileName;

    String sourceCurrencyCode;

    private Path latestFilePath;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        Path directory = Paths.get("src/main/resources/data");
        Optional<Path> latestFile = FileUtil.getLatestFile(directory);

        latestFile.ifPresent(path -> {
            System.out.println("Processing latest file: " + path.getFileName());
            // Add your file processing logic here
            latestFileName = String.valueOf(path.getFileName());
            String sourceCurrencyCode;

        });

//        chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("latestFileName", latestFileName);

        return RepeatStatus.FINISHED;
    }

    public String getLatestFileName() {
        return latestFileName;
    }

    public Path getLatestFilePath() {
        return latestFilePath;
    }
}
