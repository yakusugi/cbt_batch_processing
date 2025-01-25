package com.springbatch.controller;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobLaunchController {

	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	@Qualifier("firstJob")
	private Job job;
	
	@GetMapping("/launchJob/{email}/{targetCurrencyCode}/{dateFrom}/{dateTo}")
	public void handle(
			@PathVariable("email") String email,
			@PathVariable("targetCurrencyCode") String targetCurrencyCode,
			@PathVariable("dateFrom") String dateFromString,
	        @PathVariable("dateTo") String dateToString
			) throws Exception {
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	    Date dateFrom = formatter.parse(dateFromString);
	    Date dateTo = formatter.parse(dateToString);
		
		JobParameters jobParameters = new JobParametersBuilder()
				.addString("email", email)
				.addString("targetCurrencyCode", targetCurrencyCode)
				.addDate("dateFrom", dateFrom)
				.addDate("dateTo", dateTo)
				.toJobParameters();
		jobLauncher.run(job, jobParameters);
		
	}
}
