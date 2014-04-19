use blog;

select topic_name, sum(favorite_count) as sum_favs from storm_tweets group by topic_name order by sum_favs desc ;
select place, topic_name, sum(favorite_count) as sum_favs from storm_tweets group by place, topic_name order by place, sum_favs desc ;
