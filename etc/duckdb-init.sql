INSTALL httpfs;

LOAD httpfs;

SET
    s3_region = 'us-east-1';

SET
    s3_url_style = 'path';

SET
    s3_endpoint = 'localhost:9000';

SET
    s3_access_key_id = 'minioadmin';

SET
    s3_secret_access_key = 'minioadmin';

SET
    s3_use_ssl = 'false';

CREATE SECRET my_secret (
    TYPE s3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    REGION 'eu-west-2',
    URL_STYLE 'path',
    USE_SSL false,
    ENDPOINT 'localhost:9000'
);
