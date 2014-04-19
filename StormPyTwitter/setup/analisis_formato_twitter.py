>>> trends = get_trending_topics_text(api)
>>> trends
[{'query': '%23XRIZenAR', 'name': '#XRIZenAR'}, {'query': '%22San+Gonzalo%22', 'name': 'San Gonzalo'}, {'query': 'Xriz', 'name': 'Xriz'}, {'query': '%22Semana+Santa%22', 'name': 'Semana Santa'}, {'query': 'Rebeca', 'name': 'Rebeca'}, {'query': 'Valdeluz', 'name': 'Valdeluz'}, {'query': '%22Zac+Efron%22', 'name': 'Zac Efron'}, {'query': '%22The+Vamps%22', 'name': 'The Vamps'}, {'query': 'Espa%C3%B1a', 'name': u'Espa\xf1a'}, {'query': 'Ucrania', 'name': 'Ucrania'}]


>>> trend_tweets = get_tweets_for_trends(api, trends, count = 2, popular = True)
>>> trend_tweets
<generator object <genexpr> at 0x7f189d806460>
>>> xs = list(trend_tweets)
>>> xs
[{'tweets': [{'text': u'A las 12:20 Cerramos el programa de @elprogramadear con #OyeNi\xf1a #XRIZenAR', 'created_at': datetime.datetime(2014, 4, 15, 9, 14, 16), 'favorite_count': 114}, {'text': u'Hoy en @elprogramadear el nuevo \xe9xito de #XRIZ \U0001f3b6 #OyeNi\xf1a \U0001f3b6\n#XRIZenAR http://t.co/qwuRFQBcZe', 'created_at': datetime.datetime(2014, 4, 15, 8, 0, 49), 'favorite_count': 98}], 'query': '%23XRIZenAR', 'name': '#XRIZenAR'}, 
{'tweets': [{'text': 'Techo del palio de San Gonzalo ! http://t.co/wHmi24VuTm', 'created_at': datetime.datetime(2014, 4, 14, 23, 40, 47), 'favorite_count': 156}, {'text': u'Hoy d\xeda grande en Sevilla!! San Gonzalo en la calle. Cristo del soberano poder ante Caifas y nuestra Se\xf1ora de la Salud. Nuestra Hermandad.', 'created_at': datetime.datetime(2014, 4, 14, 9, 35, 16), 'favorite_count': 127}], 'query': '%22San+Gonzalo%22', 'name': 'San Gonzalo'}, {'tweets': [{'text': u'Hoy en @elprogramadear el nuevo \xe9xito de #XRIZ \U0001f3b6 #OyeNi\xf1a \U0001f3b6\n#XRIZenAR http://t.co/qwuRFQBcZe', 'created_at': datetime.datetime(2014, 4, 15, 8, 0, 49), 'favorite_count': 98}, {'text': u'RT para "Bady sal en el pr\xf3ximo Video de @OfficialXriz"\nFAV para "Bady desnudate en el pr\xf3ximo Video de XRIZ"\nJAJA! No d\xe9is FAV, no lo har\xe9.', 'created_at': datetime.datetime(2014, 4, 8, 16, 4, 3), 'favorite_count': 71}], 'query': 'Xriz', 'name': 'Xriz'}, {'tweets': [{'text': 'La Semana Santa es un buen momento para confesarse y retomar el camino correcto.', 'created_at': datetime.datetime(2014, 4, 14, 8, 14, 9), 'favorite_count': 4360}, {'text': 'No quiero hacer spoilers a los que no hayan visto la Semana Santa, pero si muere uno de los protagonistas el viernes, esperad al domingo.', 'created_at': datetime.datetime(2014, 4, 13, 19, 22, 13), 'favorite_count': 827}], 'query': '%22Semana+Santa%22', 'name': 'Semana Santa'}, {'tweets': [{'text': u'#GalaSV5 \xa1GENIAL! \xa1No ha habido un momentazo mejor! La madre de Rebeca en pleno letargo&gt; http://t.co/pJXQsg3jSG http://t.co/NLbSjZS6ZU', 'created_at': datetime.datetime(2014, 4, 14, 23, 30, 30), 'favorite_count': 468}, {'text': u'\U0001f631\U0001f631\U0001f631\U0001f631\U0001f631\U0001f631\U0001f631\U0001f631\U0001f631\U0001f631\U0001f631jajajajajajaja Dios m\xedo!!!!! La madre de de Rebeca dormida en plato!!! Jajajjajajajajajajja #GalaSV5', 'created_at': datetime.datetime(2014, 4, 14, 23, 30, 15), 'favorite_count': 165}], 'query': 'Rebeca', 'name': 'Rebeca'}, {'tweets': [{'text': u'#\xdaltimaHora  Queda en libertad el profesor del Valdeluz acusado de abusos sexuales http://t.co/QYD7OKxqTa', 'created_at': datetime.datetime(2014, 4, 15, 10, 20, 7), 'favorite_count': 9}, {'text': u'#\xdaLTIMAHORA Dejan en libertad al profesor del colegio #Valdeluz acusado de siete delitos de abuso sexual http://t.co/R9UPwoIUkM', 'created_at': datetime.datetime(2014, 4, 15, 10, 11, 22), 'favorite_count': 6}], 'query': 'Valdeluz', 'name': 'Valdeluz'}, {'tweets': [{'text': 'Zac Efron deslumbra en los MTV Movie Award con sus abdominales http://t.co/pPz0AYWy7e http://t.co/VjkaGULH5G', 'created_at': datetime.datetime(2014, 4, 14, 8, 4, 44), 'favorite_count': 1632}, {'text': 'Zac Efron se quita la camiseta en los MTV Awards 2014 http://t.co/8qoDi5Doz5 http://t.co/xiIZBmHaHP', 'created_at': datetime.datetime(2014, 4, 14, 12, 47, 29), 'favorite_count': 818}], 'query': '%22Zac+Efron%22', 'name': 'Zac Efron'}, {'tweets': [{'text': u'Hoy a la venta MEET THE VAMPS el esperad\xedsimo \xe1lbum debut de @thevampsband http://t.co/EtUvH7Zouo http://t.co/9QXlipvzE7', 'created_at': datetime.datetime(2014, 4, 15, 7, 50, 15), 'favorite_count': 126}, {'text': 'Ver a The Vamps haciendo firmas o 5SOS tocando para pocas Fans me recuerda a 1D al principio y quiero que vuelvan esos tiempos.', 'created_at': datetime.datetime(2014, 4, 14, 15, 55, 22), 'favorite_count': 111}], 'query': '%22The+Vamps%22', 'name': 'The Vamps'}, {'tweets': [{'text': u'Tal d\xeda como hoy, un 14 de abril de 1931, se declaraba la II Rep\xfablica en Espa\xf1a. #14APorLaRep\xfablica #APorLaTercera http://t.co/qVqq6w22rp', 'created_at': datetime.datetime(2014, 4, 13, 22, 0, 9), 'favorite_count': 1627}, {'text': u'Ya es Oficial! El 3er \xe1lbum de @aurynoficial se lanzar\xe1 simult\xe1neamente en Espa\xf1a, M\xe9xico, Argentina y m\xe1s pa\xedses latinoamericanos!!!', 'created_at': datetime.datetime(2014, 4, 14, 12, 44, 39), 'favorite_count': 932}], 'query': 'Espa%C3%B1a', 'name': u'Espa\xf1a'}, {'tweets': [{'text': u'Tengo la sensaci\xf3n de que este a\xf1o Rusia y Ucrania no van a darse los 12 puntos en Eurovisi\xf3n.', 'created_at': datetime.datetime(2014, 4, 14, 8, 53, 36), 'favorite_count': 449}, {'text': 'Ucrania y Rusia se van a liar a tortas y si yo me meto, no va a ser para separar.', 'created_at': datetime.datetime(2014, 4, 13, 22, 9, 57), 'favorite_count': 185}], 'query': 'Ucrania', 'name': 'Ucrania'}]

## FORMATO DE LOS TWEETS
# Los objetos que corresponden a tweets son de la clase tweepy.models.Status, supongo q viene de status update
>>> api.search(q = 'Ucrania')[0].__class__
<class 'tweepy.models.Status'>

>>> api.search(q = 'Ucrania')[0].__dict__
{'contributors': None, 
'truncated': False,
 'text': u'RT @castorphoto: O que \xe9 certo \xe9 que os servi\xe7os secretos da Ucr\xe2nia s\xe3o comandados pela CIA j\xe1 h\xe1 22 anos, e que as autoridades... http://\u2026', <--
  'in_reply_to_status_id': None, 
  'id': 456032217714933760,  
  'favorite_count': 0, <--
  '_api': <tweepy.api.API object at 0x7f189d80b250>, 
  'author': <tweepy.models.User object at 0x2b18950>,  <-- 
  'retweeted': False, <--
  'coordinates': None,
  'entities': {'symbols': [], 'user_mentions': [{'id': 59314487, 'indices': [3, 15], 'id_str': '59314487', 'screen_name': 'castorphoto', 'name': 'Castor Filho'}], 'hashtags': [], 'urls': [{'url': 'http://t.co/wkfbGhNF6a', 'indices': [139, 140], 'expanded_url': 'http://fb.me/6mYuWKIq5', 'display_url': 'fb.me/6mYuWKIq5'}]}, 
        <-- interesante las hashtags, que salen con [(tweet.text, [ hashtag["text"] for hashtag in tweet.entities["hashtags"]]) for tweet in  api.search(q = 'Ucrania') ]
   'in_reply_to_screen_name': None, <-- usuario legible
   'in_reply_to_user_id': None, 
   'retweet_count': 6, <--
   'id_str': '456032217714933760', 
   'favorited': False, 
   'retweeted_status': <tweepy.models.Status object at 0x2b18910>, 
   'source_url': None, 'user': <tweepy.models.User object at 0x2b18950>, 
   'geo': None,
   'in_reply_to_user_id_str': None, 
   'possibly_sensitive': False,  <-- NSFW mark
   'lang': 'pt', <-- 
   'created_at': datetime.datetime(2014, 4, 15, 11, 32, 8),  <--
   'in_reply_to_status_id_str': None, 
   'place': None, <-- usar "full_name", ver abajo
   'source': 'web', <--
   'metadata': {'iso_language_code': 'pt', 'result_type': 'recent'}}

>>> api.search(q = 'Ucrania')[0].author.__dict__
{'follow_request_sent': False, 'profile_use_background_image': True, 'id': 144092399, '_api': <tweepy.api.API object at 0x7f189d80b250>, 'verified': False, 'profile_text_color': '333333', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/1375086256/__________normal.jpg', 'profile_sidebar_fill_color': 'DDEEF6', 'is_translator': False, 'geo_enabled': True, 'entities': {'description': {'urls': []}}, 'followers_count': 550, 'protected': False, 'location': 'Moscow, USSR', 'default_profile_image': False, 'id_str': '144092399', 'lang': 'es', 'utc_offset': 14400, 'statuses_count': 5205, 'description': u'Cheburashka tuitea en sus ratos libres sobre Rusia y la exURSS. No cobra por ello (desgraciadamente:) y no promociona nada (bueno s\xed, dibujos animados). Welcome', 'friends_count': 163, 'profile_link_color': '0084B4', 'profile_image_url': 'http://pbs.twimg.com/profile_images/1375086256/__________normal.jpg', 'notifications': False, 'profile_background_image_url_https': 'https://pbs.twimg.com/profile_background_images/260501233/fondo3.jpg', 'profile_background_color': 'C0DEED', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/144092399/1356120103', 'profile_background_image_url': 'http://pbs.twimg.com/profile_background_images/260501233/fondo3.jpg', 
    'name': 'ChebuRusia', 'is_translation_enabled': False, 'profile_background_tile': False, 'favourites_count': 2053, 
    'screen_name': 'eraserusia', <-- suficiente esto
     'url': None, 'created_at': datetime.datetime(2010, 5, 15, 8, 11, 31), 'contributors_enabled': False, 'time_zone': 'Moscow', 'profile_sidebar_border_color': 'C0DEED', 'default_profile': False, 'following': False, 'listed_count': 35}

>>> [(tweet.place.__dict__ if tweet.place != None else None, tweet.text)  for tweet in api.search(q = 'Semana')]
....

({'_api': <tweepy.api.API object at 0x7f189d80b250>, 'country_code': 'ES', 'url': 'https://api.twitter.com/1.1/geo/id/1a27537478dd8e38.json', 'country': u'Espa\xf1a', 'place_type': 'city', 'bounding_box': <tweepy.models.BoundingBox object at 0x2b15990>, 'contained_within': [], 'full_name': 'Barcelona, Barcelona', <--
    'attributes': {}, 'id': '1a27537478dd8e38', 'name': 'Barcelona'}, u'que diferente la semana santa de un a\xf1o a otro,')