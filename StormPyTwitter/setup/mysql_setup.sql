use blog;
CREATE TABLE IF NOT EXISTS storm_tweets (
    text VARCHAR(140) NOT NULL,
    favorite_count SMALLINT UNSIGNED,
    retweeted BOOL,
    in_reply_to_screen_name VARCHAR(20),
    retweet_count SMALLINT UNSIGNED,
    possibly_sensitive BOOL,
    lang VARCHAR(3),
    created_at TIMESTAMP NOT NULL,
    source VARCHAR(5),
    author_screen_name VARCHAR(20) NOT NULL,
    -- Sort and concat before insert
    hashtags_texts VARCHAR(100),
    place_full_name VARCHAR(50),
    topic_name VARCHAR(20),
    PRIMARY KEY(created_at, author_screen_name)
);

