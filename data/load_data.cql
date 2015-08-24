use killrvideo;

COPY users
(userid, firstname, lastname,email, created_date)
FROM 'users.csv';

COPY videos
(videoid, userid, name, description, location, location_type, tags, added_date)
FROM 'videos.csv';

COPY comments_by_user
(userid, commentid, videoid, comment)
FROM 'comments_by_user.csv';

COPY comments_by_video
(videoid, commentid, userid, comment)
FROM 'comments_by_video.csv';

COPY user_videos
(userid, added_date, videoid, name, preview_image_location)
FROM 'user_videos.csv';

source 'video_rating_load.cql';

COPY video_ratings_by_user
(videoid, userid, rating)
FROM 'video_ratings_by_user.csv';

COPY videos_by_tag
(tag, videoid, added_date, name, preview_image_location, tagged_date)
FROM 'videos_by_tag.csv';
