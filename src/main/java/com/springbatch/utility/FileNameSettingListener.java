package com.springbatch.utility;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.batch.runtime.StepExecution;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class FileNameSettingListener implements StepExecutionListener {

    private String fileName;
	@Override
	public void beforeStep(org.springframework.batch.core.StepExecution stepExecution) {
		// TODO Auto-generated method stub
		 // Generate the file name
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String formattedDate = dateFormat.format(new Date());
        fileName = "src/main/resources/data/Product_Details_" + formattedDate + ".csv";

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

