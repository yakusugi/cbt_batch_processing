package com.spring.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.springbatch.domain.UserSpending;

public class UserSpendingItemProcessor implements ItemProcessor<UserSpending, UserSpending> {

    @Override
    public UserSpending process(UserSpending item) throws Exception {
        // Print the calculated result (or any other properties) to the console
        System.out.println("Processed User Spending: " + item);

        // Return the item to pass it to the writer
        return item;
    }
}
