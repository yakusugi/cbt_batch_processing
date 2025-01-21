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

        if (latestFile.isPresent()) {
            latestFilePath = latestFile.get();  // Update latestFilePath
            latestFileName = latestFilePath.getFileName().toString();  // Update latestFileName

            System.out.println("Processing latest file: " + latestFileName);
            System.out.println("Latest File Name: " + latestFileName);

            // Optionally, store this information in the execution context for later steps
            chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("latestFileName", latestFileName);
//            chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().put("latestFilePath", latestFilePath.toString());
        } else {
            System.out.println("No files found in the directory: " + directory);
        }
        return RepeatStatus.FINISHED;
    }

    public String getLatestFileName() {
        return latestFileName;
    }

    public Path getLatestFilePath() {
        return latestFilePath;
    }
}
