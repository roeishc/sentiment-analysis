package com.handson.sentiment_analysis.controller;

import com.handson.sentiment_analysis.kafka.AppKafkaSender;
import com.handson.sentiment_analysis.news.AppNewsStream;
import com.handson.sentiment_analysis.nlp.SentimentAnalyzer;
import org.joda.time.DateTime;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.text.SimpleDateFormat;
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
    private KafkaReceiver<String, String> kafkaReceiver;


    @RequestMapping(path = "/grouped", method = RequestMethod.GET)
    public  @ResponseBody Flux<String> grouped(@RequestParam(defaultValue = "obama") String text,
                                               @RequestParam(defaultValue = "3") Integer timeWindowSec) throws InterruptedException {

        var flux = kafkaReceiver.receive().map(message -> message.value());
        appNewsStream.filter(text).map((x)-> kafkaSender.send(x, APP_TOPIC)).subscribe();

        return flux.map(x -> new TimeAndMessage(DateTime.now(), x))
                .window(Duration.ofSeconds(timeWindowSec))
                .flatMap(window -> toArrayList(window))
                .map(y -> {
                    if (y.isEmpty())
                        return "size: 0 <br>";
                    return "time: " + y.get(0).curTime + ", size: " + y.size() + "<br>";
                });
    }

    @RequestMapping(path = "/sentiment", method = RequestMethod.GET)
    public  @ResponseBody Flux<String> sentiment(@RequestParam(defaultValue = "obama") String text,
                                                 @RequestParam(defaultValue = "3") Integer timeWindowSec) throws InterruptedException {
        var flux = kafkaReceiver.receive().map(message -> message.value());
        appNewsStream.filter(text).map((x)-> kafkaSender.send(x, APP_TOPIC)).subscribe();

        return flux.map(x-> new TimeAndMessage(DateTime.now(), x))
                .window(Duration.ofSeconds(timeWindowSec))
                .flatMap(window -> toArrayList(window))
                .map(items -> {
                    if (items.size() > 10)
                        return "size: " + items.size() + "<br>";
                    System.out.println("size: " + items.size());
                    double avg = items.stream().map(x-> sentimentAnalyzer.analyze(x.message))
                            .mapToDouble(y -> y).average().orElse(0.0);
                    if (items.isEmpty())
                        return "EMPTY<br>";
                    return items.size() + " messages, sentiment = " + avg + "<br>";
                });
    }

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

    @RequestMapping(path = "/getAnalysis", method = RequestMethod.GET)
    public @ResponseBody Mono<String> getAnalysis(@RequestParam String text)  {
        Double score =  sentimentAnalyzer.analyze(text);
        return Mono.just("" + score);
    }

    private <T> Mono<ArrayList<T>> toArrayList(Flux<T> source) {
        return source.reduce(new ArrayList<>(), (a, b) -> {
            a.add(b);
            return a;
        });
    }

    static class TimeAndMessage {

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd, HH:mm:ss, z");

        DateTime curTime;

        String message;

        public TimeAndMessage(DateTime curTime, String message) {
            this.curTime = curTime;
            this.message = message;
        }

        @Override
        public String toString() {
            return "TimeAndMessage{" +
                    "formatter=" + formatter +
                    ", curTime=" + curTime +
                    ", message='" + message + '\'' +
                    '}';
        }
    }
}