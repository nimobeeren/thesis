#!/bin/env bash

## Modify the line below to the directory that contains the SNB dataset
export LDBC_SNB_DATA_DIR=/home/tigergraph/mydata/snb

gsql -g ldbc_snb "run loading job load_ldbc_snb using 
v_organisation_file=\"${LDBC_SNB_DATA_DIR}/static/organisation_0_0.csv\", 
v_place_file=\"${LDBC_SNB_DATA_DIR}/static/place_0_0.csv\",
v_tag_file=\"${LDBC_SNB_DATA_DIR}/static/tag_0_0.csv\", 
v_tagclass_file=\"${LDBC_SNB_DATA_DIR}/static/tagclass_0_0.csv\",
v_comment_file=\"${LDBC_SNB_DATA_DIR}/dynamic/comment_0_0.csv\", 
v_forum_file=\"${LDBC_SNB_DATA_DIR}/dynamic/forum_0_0.csv\",
v_person_file=\"${LDBC_SNB_DATA_DIR}/dynamic/person_0_0.csv\",
v_post_file=\"${LDBC_SNB_DATA_DIR}/dynamic/post_0_0.csv\",

organisation_is_located_in_place_file=\"${LDBC_SNB_DATA_DIR}/static/organisation_isLocatedIn_place_0_0.csv\",
place_is_part_of_place_file=\"${LDBC_SNB_DATA_DIR}/static/place_isPartOf_place_0_0.csv\",
tagclass_is_subclass_of_tagclass_file=\"${LDBC_SNB_DATA_DIR}/static/tagclass_isSubclassOf_tagclass_0_0.csv\",
tag_has_type_tagclass_file=\"${LDBC_SNB_DATA_DIR}/static/tag_hasType_tagclass_0_0.csv\",
comment_has_creator_person_file=\"${LDBC_SNB_DATA_DIR}/dynamic/comment_hasCreator_person_0_0.csv\",
comment_has_tag_tag_file=\"${LDBC_SNB_DATA_DIR}/dynamic/comment_hasTag_tag_0_0.csv\",
comment_is_located_in_place_file=\"${LDBC_SNB_DATA_DIR}/dynamic/comment_isLocatedIn_place_0_0.csv\",
comment_reply_of_comment_file=\"${LDBC_SNB_DATA_DIR}/dynamic/comment_replyOf_comment_0_0.csv\",
comment_reply_of_post_file=\"${LDBC_SNB_DATA_DIR}/dynamic/comment_replyOf_post_0_0.csv\",
forum_container_of_post_file=\"${LDBC_SNB_DATA_DIR}/dynamic/forum_containerOf_post_0_0.csv\",
forum_has_member_person_file=\"${LDBC_SNB_DATA_DIR}/dynamic/forum_hasMember_person_0_0.csv\",
forum_has_moderator_person_file=\"${LDBC_SNB_DATA_DIR}/dynamic/forum_hasModerator_person_0_0.csv\",
forum_has_tag_tag_file=\"${LDBC_SNB_DATA_DIR}/dynamic/forum_hasTag_tag_0_0.csv\",
person_has_interest_tag_file=\"${LDBC_SNB_DATA_DIR}/dynamic/person_hasInterest_tag_0_0.csv\",
person_is_located_in_place_file=\"${LDBC_SNB_DATA_DIR}/dynamic/person_isLocatedIn_place_0_0.csv\",
person_knows_person_file=\"${LDBC_SNB_DATA_DIR}/dynamic/person_knows_person_0_0.csv\", 
person_likes_comment_file=\"${LDBC_SNB_DATA_DIR}/dynamic/person_likes_comment_0_0.csv\", 
person_likes_post_file=\"${LDBC_SNB_DATA_DIR}/dynamic/person_likes_post_0_0.csv\", 
person_study_at_organisation_file=\"${LDBC_SNB_DATA_DIR}/dynamic/person_studyAt_organisation_0_0.csv\",
person_work_at_organisation_file=\"${LDBC_SNB_DATA_DIR}/dynamic/person_workAt_organisation_0_0.csv\",
post_has_creator_person_file=\"${LDBC_SNB_DATA_DIR}/dynamic/post_hasCreator_person_0_0.csv\",
post_has_tag_tag_file=\"${LDBC_SNB_DATA_DIR}/dynamic/post_hasTag_tag_0_0.csv\",
post_is_located_in_place_file=\"${LDBC_SNB_DATA_DIR}/dynamic/post_isLocatedIn_place_0_0.csv\""
