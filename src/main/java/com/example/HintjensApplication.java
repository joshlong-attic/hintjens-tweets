package com.example;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InjectionPoint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.social.twitter.api.HashTagEntity;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
public class HintjensApplication {

    @Bean
    TwitterTemplate twitterTemplate(@Value("${hintjens.consumerKey}") String consumerKey,
                                    @Value("${hintjens.consumerSecret}") String consumerSecret,
                                    @Value("${hintjens.accessToken}") String accessToken,
                                    @Value("${hintjens.accessTokenSecret}") String accessTokenSecret) {
        return new TwitterTemplate(consumerKey, consumerSecret, accessToken, accessTokenSecret);
    }

    @Bean
    @Scope("prototype")
    Logger logger(InjectionPoint ip) {
        return Logger.getLogger(ip.getMember().getDeclaringClass().getName());
    }

    @Bean
    CommandLineRunner tweets(Logger logger, TwitterTemplate twitterTemplate, JdbcTemplate jdbcTemplate) {
        return args -> {
            String screenName = "hintjens";
            int pageSize = 200;
            List<Tweet> userTimeline = twitterTemplate.timelineOperations()
                    .getUserTimeline(screenName, pageSize);
            long maxId;
            while (userTimeline.size() != 0 ) {
                maxId = userTimeline.stream()
                        .map(Tweet::getId)
                        .reduce((tweet, tweet2) -> tweet < tweet2 ? tweet : tweet2)
                        .orElse(0L);
                userTimeline = twitterTemplate.timelineOperations()
                        .getUserTimeline(screenName, pageSize, 0L, maxId - 1);
                addAllTweets(jdbcTemplate, userTimeline);
                logger.info("added " + userTimeline.size() + " records.");
            }
        };
    }

    private void addAllTweets(JdbcTemplate template, List<Tweet> tweets) {
        List<Object[]> list = tweets.stream().map(tweet -> new Object[]{tweet.getText(),
                tweet.getFromUser(),
                tweet.getCreatedAt(),
                tweet.getId(),
                tweet.getProfileImageUrl(),
                tweet.getIdStr(),
                tweet.getLanguageCode(),
                tweet.getUser().getName(),
                tweet.getInReplyToStatusId(),
                tweet.getInReplyToUserId(),
                tweet.getInReplyToScreenName(),
                tweet.getToUserId(),
                tweet.getEntities().getHashTags()
                        .stream()
                        .map(HashTagEntity::getText)
                        .collect(Collectors.joining(" ")),
                tweet.getRetweetCount(),
                tweet.getRetweetedStatus() != null ? tweet.getRetweetedStatus().getId() : null,
                tweet.getUnmodifiedText(),
                tweet.getSource()})
                .collect(Collectors.toList());

        template.batchUpdate("insert into tweets(" +
                "text, " +
                "from_user, " +
                "created_at, " +
                "tweet_id, " +
                "profile_image_url," +
                "id_string," +
                "language_code, " +
                "username," +
                "in_reply_to_status_id , " +
                "in_reply_to_user_id," +
                "in_reply_to_screename," +
                "to_user_id," +
                "hashtags," +
                "retweeted_count," +
                "retweeted_status_id," +
                "unmodified_text," +
                "source" +
                ") values ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", list);
    }

    public static void main(String[] args) {
        SpringApplication.run(HintjensApplication.class, args);
    }
}
