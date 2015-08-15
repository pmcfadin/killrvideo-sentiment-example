use killrvideo;


COPY users
(userid, firstname, lastname,email, created_date)
TO 'users.csv';

COPY videos
(videoid, userid, name, description, location, location_type, tags, added_date)
TO 'videos.csv';

COPY comments_by_user
(userid, commentid, videoid, comment)
TO 'comments_by_user.csv';

COPY comments_by_video
(videoid, commentid, userid, comment)
TO 'comments_by_video.csv';

COPY tags_by_letter
(first_letter, tag)
TO 'tags_by_letter.csv';

COPY user_videos
(userid, added_date, videoid, name, preview_image_location)
TO 'tags_by_letter.csv';

COPY  video_rating
(videoid, rating_counter, rating_total)
TO 'video_rating.csv';

COPY video_ratings_by_user
(videoid, userid, rating)
TO 'video_ratings_by_user.csv';

COPY videos_by_tag
(tag, videoid, added_date, name, preview_image_location, tagged_date)
TO 'videos_by_tag.csv';
