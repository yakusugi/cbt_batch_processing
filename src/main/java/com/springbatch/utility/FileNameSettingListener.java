package com.springbatch.utility;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.batch.runtime.StepExecution;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class FileNameSettingListener implements StepExecutionListener {

    private String fileName;

    @Value("${app.output.directory}")
    private String outputDirectory;

	@Override
	public void beforeStep(org.springframework.batch.core.StepExecution stepExecution) {
        String targetCurrencyCode = stepExecution.getJobParameters().getString("targetCurrencyCode");
		 // Generate the file name
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String formattedDate = dateFormat.format(new Date());
        fileName = outputDirectory + "/" + targetCurrencyCode + "_Product_Details_" + formattedDate + ".csv";

        // Store the file name in the execution context
        stepExecution.getExecutionContext().putString("csvFileName", fileName);
		
	}


	@Override
	public ExitStatus afterStep(org.springframework.batch.core.StepExecution stepExecution) {
		// TODO Auto-generated method stub
		return ExitStatus.COMPLETED;
	}

    public String getFileName() {
        return fileName;
    }

}

