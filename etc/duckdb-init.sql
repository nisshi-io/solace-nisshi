-- Copyright (c) Solace Corporation
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
