package com.handson.sentiment_analysis.controller;

import com.handson.sentiment_analysis.kafka.AppKafkaSender;
import com.handson.sentiment_analysis.news.AppNewsStream;
import com.handson.sentiment_analysis.nlp.SentimentAnalyzer;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Duration;
import java.util.ArrayList;

import static com.handson.sentiment_analysis.kafka.KafkaTopicConfig.APP_TOPIC;

@RestController
public class AppController {

    @Autowired
    private SentimentAnalyzer sentimentAnalyzer;

    @Autowired
    private AppNewsStream appNewsStream;

    @Autowired
    private AppKafkaSender kafkaSender;

    @Autowired
    private KafkaReceiver<String,String> kafkaReceiver;


    @RequestMapping(path = "/sendKafka", method = RequestMethod.GET)
    public  @ResponseBody Mono<String> sendText(String text)  {
        kafkaSender.send(text, APP_TOPIC);
        return Mono.just("OK");
    }

    @RequestMapping(path = "/getKafka", method = RequestMethod.GET)
    public  @ResponseBody  Flux<String> getKafka()  {
        return kafkaReceiver.receive().map(x -> x.value() + "<br>");
    }

    @RequestMapping(path = "/startNewsStream", method = RequestMethod.GET)
    public @ResponseBody Flux<String> start(String text) throws Exception {
        return appNewsStream.filter(text)
                .window(Duration.ofSeconds(3))
                .flatMap(window -> toArrayList(window))
                .map(message -> {
                    if (message.isEmpty())
                        return "size: 0<br>";
                    return "size: " + message.size() + "<br>";
                });
    }

    @RequestMapping(path = "/stopNewsStream", method = RequestMethod.GET)
    public @ResponseBody Mono<String> stop(String text) throws Exception {
        appNewsStream.shutdown();
        return Mono.just("shutdown");
    }

    private <T> Mono<ArrayList<T>> toArrayList(Flux<T> source) {
        return source.reduce(new ArrayList<>(), (a, b) -> {
            a.add(b);
            return a;
        });
    }

    @RequestMapping(path = "/getAnalysis", method = RequestMethod.GET)
    public @ResponseBody Mono<String> getAnalysis(@RequestParam String text)  {
        Double score =  sentimentAnalyzer.analyze(text);
        return Mono.just("" + score);
    }
}