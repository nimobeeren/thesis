#!/bin/env bash
set -euo pipefail

# Replace headers for all node files
sed -i "1s/.*/id:ID(Organisation)|:LABEL|name|url/" $1/static/organisation_0_0.csv
sed -i "1s/.*/id:ID(Place)|name|url|:LABEL/" $1/static/place_0_0.csv
sed -i "1s/.*/id:ID(Tag)|name|url/" $1/static/tag_0_0.csv
sed -i "1s/.*/id:ID(TagClass)|name|url/" $1/static/tagclass_0_0.csv
sed -i "1s/.*/id:ID(Comment)|creationDate:datetime|locationIP|browserUsed|content|length:int/" $1/dynamic/comment_0_0.csv
sed -i "1s/.*/id:ID(Forum)|title|creationDate:datetime/" $1/dynamic/forum_0_0.csv
sed -i "1s/.*/id:ID(Person)|firstName|lastName|gender|birthday:date|creationDate:datetime|locationIP|browserUsed/" $1/dynamic/person_0_0.csv
sed -i "1s/.*/id:ID(Post)|imageFile|creationDate:datetime|locationIP|browserUsed|language|content|length:int/" $1/dynamic/post_0_0.csv

# Replace headers for all edge files
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/static/organisation_isLocatedIn_place_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/static/place_isPartOf_place_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/static/tagclass_isSubclassOf_tagclass_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/static/tag_hasType_tagclass_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/comment_hasCreator_person_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/comment_hasTag_tag_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/comment_isLocatedIn_place_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/comment_replyOf_comment_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/comment_replyOf_post_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/forum_containerOf_post_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id|joinDate/:START_ID(\1)|:END_ID(\2)|creationDate:datetime/" $1/dynamic/forum_hasMember_person_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/forum_hasModerator_person_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/forum_hasTag_tag_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/person_hasInterest_tag_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/person_isLocatedIn_place_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id|creationDate/:START_ID(\1)|:END_ID(\2)|creationDate:datetime/" $1/dynamic/person_knows_person_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id|creationDate/:START_ID(\1)|:END_ID(\2)|creationDate:datetime/" $1/dynamic/person_likes_comment_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id|creationDate/:START_ID(\1)|:END_ID(\2)|creationDate:datetime/" $1/dynamic/person_likes_post_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id|classYear/:START_ID(\1)|:END_ID(\2)|classYear:int/" $1/dynamic/person_studyAt_organisation_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id|workFrom/:START_ID(\1)|:END_ID(\2)|workFrom:int/" $1/dynamic/person_workAt_organisation_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/post_hasCreator_person_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/post_hasTag_tag_0_0.csv
sed -i "1s/\(\w\+\)\.id|\(\w\+\)\.id/:START_ID(\1)|:END_ID(\2)/" $1/dynamic/post_isLocatedIn_place_0_0.csv

# Capitalize :LABEL values
sed -i "s/company/Company/g" $1/static/organisation_0_0.csv
sed -i "s/university/University/g" $1/static/organisation_0_0.csv
sed -i "s/city/City/g" $1/static/place_0_0.csv
sed -i "s/country/Country/g" $1/static/place_0_0.csv
sed -i "s/continent/Continent/g" $1/static/place_0_0.csv
