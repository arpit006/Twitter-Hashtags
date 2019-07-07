package com.kafka.twitterkafka.hosebird.controller;

import com.kafka.twitterkafka.hosebird.service.TwitterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@RestController
@RequestMapping("/twitter")
public class TwitterController {

    @Autowired
    private TwitterService twitterService;

    @PostMapping(value = "",/* consumes = "application/json", */produces = "application/json")
    public void queryThisHashTag(@RequestBody String hashTag) {
        twitterService.queryThisHashTag(hashTag);
    }
}
