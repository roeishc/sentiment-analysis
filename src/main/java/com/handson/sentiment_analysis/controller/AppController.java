package com.handson.sentiment_analysis.controller;

import com.handson.sentiment_analysis.nlp.SentimentAnalyzer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
public class AppController {

    @Autowired
    private SentimentAnalyzer sentimentAnalyzer;

    @RequestMapping(path = "/getAnalysis", method = RequestMethod.GET)
    public @ResponseBody Mono<String> getAnalysis(@RequestParam String text)  {
        Double score =  sentimentAnalyzer.analyze(text);
        return Mono.just("" + score);
    }
}