use blog;
CREATE TABLE IF NOT EXISTS storm_tweets (
    -- experimentally 140 is not enough
    text VARCHAR(200) NOT NULL,
    favorite_count INT UNSIGNED,
    retweeted BOOL,
    in_reply_to_screen_name VARCHAR(50),
    retweet_count INT UNSIGNED,
    possibly_sensitive BOOL,
    lang VARCHAR(3),
    created_at DATETIME NOT NULL,
    source VARCHAR(50),
    author_screen_name VARCHAR(50) NOT NULL,
    -- Sort and concat before insert
    hashtags_texts VARCHAR(300),
    place VARCHAR(50),
    topic_name VARCHAR(50),
    -- number of times the tweet for the key has been found in the popularity list
    hits INT UNSIGNED,
    PRIMARY KEY(author_screen_name, created_at)
);
