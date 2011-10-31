CREATE NAMESPACE "/hypertable-samples/microblog";

USE "/hypertable-samples/microblog";

CREATE TABLE profile ('guid' MAX_VERSIONS 1 TIME_ORDER DESC, 
                      'display_name' MAX_VERSIONS 1, 
                      'password' MAX_VERSIONS 1, 
                      'email' MAX_VERSIONS 1, 
                      'avatar' MAX_VERSIONS 1,
                      'location' MAX_VERSIONS 1, 
                      'webpage' MAX_VERSIONS 1, 
                      'bio' MAX_VERSIONS 1);

# user 'alice', password is "123"
INSERT INTO profile VALUES ('alice', 
            'guid', 'alice');
INSERT INTO profile VALUES ('alice', 
            'display_name', 'Alice Z.');
INSERT INTO profile VALUES ('alice', 
            'password', '202cb962ac59075b964b07152d234b70');
INSERT INTO profile VALUES ('alice', 
            'email', 'alice@hypertable.com');
INSERT INTO profile VALUES ('alice', 
            'location', 'Munich, Germany');
INSERT INTO profile VALUES ('alice', 
            'bio', 'My name is Alice!');
INSERT INTO profile VALUES ('alice', 
            'webpage', 'http://www.hypertable.com');
INSERT INTO profile VALUES ('alice', 
            'avatar', 'http://fastcache.gawkerassets.com/assets/images/9/2009/02/American_McGee_Alice_box.gif');

# user 'bob', password is empty
INSERT INTO profile VALUES ('bob', 
            'guid', 'bob');
INSERT INTO profile VALUES ('bob', 
            'display_name', 'Bob Y.');
INSERT INTO profile VALUES ('bob', 
            'password', 'd41d8cd98f00b204e9800998ecf8427e');
INSERT INTO profile VALUES ('bob', 
            'email', 'bob@hypertable.com');
INSERT INTO profile VALUES ('bob', 
            'location', 'Redwood City, CA');
INSERT INTO profile VALUES ('bob', 
            'bio', 'My name is Bob!');
INSERT INTO profile VALUES ('bob', 
            'webpage', 'http://www.hypertable.com');
INSERT INTO profile VALUES ('bob', 
            'avatar', 'http://toastytech.com/guis/bobboot1.gif');

# user 'carl', password is empty
INSERT INTO profile VALUES ('carl', 
            'guid', 'carl');
INSERT INTO profile VALUES ('carl', 
            'display_name', 'Carl X.');
INSERT INTO profile VALUES ('carl', 
            'password', 'd41d8cd98f00b204e9800998ecf8427e');
INSERT INTO profile VALUES ('carl', 
            'email', 'carl@hypertable.com');
INSERT INTO profile VALUES ('carl', 
            'location', 'Redwood City, CA');
INSERT INTO profile VALUES ('carl', 
            'bio', 'My name is Carl.');
INSERT INTO profile VALUES ('carl', 
            'webpage', 'http://www.hypertable.com');
INSERT INTO profile VALUES ('carl', 
            'avatar', 'http://assets.sbnation.com/assets/668221/Carl_waving.gif');

CREATE TABLE user ('following' MAX_VERSIONS 1, 
                   'following_history',
                   'following_count' COUNTER,
                   'followers' MAX_VERSIONS 1, 
                   'followers_history',
                   'followers_count' COUNTER,
                   'follow_stream', 
                   'follow_stream_count' COUNTER,
                   'my_stream', 
                   'my_stream_count' COUNTER); 
INSERT INTO user VALUES ('alice', 'following:bob', ''),
                    ('bob', 'followers:alice', ''),
                    ('alice', 'following_count', '+1'),
                    ('bob', 'followers_count', '+1'),
                    ('alice', 'following_history', 'bob'),
                    ('bob', 'followers_history', 'alice');
INSERT INTO user VALUES ('alice', 'following:carl', ''),
                    ('carl', 'followers:alice', ''),
                    ('alice', 'following_count', '+1'),
                    ('carl', 'followers_count', '+1'),
                    ('alice', 'following_history', 'carl'),
                    ('carl', 'followers_history', 'alice');
INSERT INTO user VALUES ('bob', 'following:alice', ''),
                    ('alice', 'followers:bob', ''),
                    ('bob', 'following_count', '+1'),
                    ('alice', 'followers_count', '+1'),
                    ('bob', 'following_history', 'alice'),
                    ('alice', 'followers_history', 'bob');

CREATE TABLE tweet ('author', 'message');
INSERT INTO tweet VALUES ('alice.1316689570000', 'message', 
                        'Alice says: Hello world!');
INSERT INTO user VALUES ('alice', 'my_stream', 'alice.1316689570000'),
                        ('alice', 'my_stream_count', '+1');
INSERT INTO user VALUES ('bob', 'follow_stream', 'alice.1316689570000'),
                        ('bob', 'follow_stream_count', '+1');
INSERT INTO tweet VALUES ('alice.1316690570000', 'message', 
                        'Alice says: Welcome to Hypertable!');
INSERT INTO user VALUES ('alice', 'my_stream', 'alice.1316690570000'),
                        ('alice', 'my_stream_count', '+1');
INSERT INTO user VALUES ('bob', 'follow_stream', 'alice.1316690570000'),
                        ('bob', 'follow_stream_count', '+1');

INSERT INTO tweet VALUES ('bob.1316689570000', 'message', 
                        'Bob says: Hello world!');
insert into user values ('bob', 'my_stream', 'bob.1316689570000'),
                        ('bob', 'my_stream_count', '+1');
INSERT INTO tweet VALUES ('bob.1316690570000', 'message', 
                        'Bob says: Welcome to Hypertable!');
INSERT INTO user VALUES ('bob', 'my_stream', 'bob.1316690570000'),
                        ('bob', 'my_stream_count', '+1');
INSERT INTO tweet VALUES ('bob.1316693570000', 'message', 
                        'Bob says: This is my 3rd message.');
INSERT INTO user VALUES ('bob', 'my_stream', 'bob.1316693570000'),
                        ('bob', 'my_stream_count', '+1');

INSERT INTO tweet VALUES ('carl.1316689570000', 'message', 
                        'Carl says: Hello world!');
INSERT INTO user VALUES ('carl', 'my_stream', 'carl.1316689570000'),
                        ('carl', 'my_stream_count', '+1');
INSERT INTO user VALUES ('alice', 'follow_stream', 'carl.1316689570000'),
                        ('alice', 'follow_stream_count', '+1');



