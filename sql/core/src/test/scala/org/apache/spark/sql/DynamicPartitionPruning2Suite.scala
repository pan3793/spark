/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.scalatest.GivenWhenThen

import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

/**
 * Test suite for the filtering ratio policy used to trigger dynamic partition pruning (DPP).
 */
class DynamicPartitionPruning2Suite
    extends QueryTest
    with SQLTestUtils
    with GivenWhenThen
    with AdaptiveSparkPlanHelper
    with SharedSparkSession {

  val tableFormat: String = "parquet"

  protected def initState(): Unit = {}

  protected def runAnalyzeColumnCommands: Boolean = true

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_tag_dd` (
        |  `playlist_id` STRING,
        |  `playlist_name` STRING,
        |  `playlist_type` STRING,
        |  `user_id` STRING,
        |  `nick_name` STRING,
        |  `create_date` STRING,
        |  `create_period` STRING,
        |  `create_playlist_duration` BIGINT,
        |  `description` STRING,
        |  `is_high_quality` STRING,
        |  `cover_status` STRING,
        |  `cover_id` STRING,
        |  `song_num` BIGINT,
        |  `online_song_num` BIGINT,
        |  `cloud_song_num` BIGINT,
        |  `play_num` BIGINT,
        |  `display_play_num` BIGINT,
        |  `collect_num` BIGINT,
        |  `share_num` BIGINT,
        |  `comment_num` BIGINT,
        |  `fee_song_num` BIGINT,
        |  `fee_level` BIGINT,
        |  `user_tag_list` STRING,
        |  `language_area` STRING,
        |  `genre_tag_list` STRING,
        |  `is_genre_matched` STRING,
        |  `is_antispam` STRING,
        |  `is_compliant` STRING,
        |  `play_cnt_1d` BIGINT,
        |  `play_unt_1d` BIGINT,
        |  `play_cnt_7d` BIGINT,
        |  `play_unt_7d` BIGINT,
        |  `play_cnt_28d` BIGINT,
        |  `play_unt_28d` BIGINT,
        |  `song_effective_play_cnt_1d` BIGINT,
        |  `song_effective_play_unt_1d` BIGINT,
        |  `song_effective_play_cnt_7d` BIGINT,
        |  `song_effective_play_unt_7d` BIGINT,
        |  `song_effective_play_cnt_28d` BIGINT,
        |  `song_effective_play_unt_28d` BIGINT,
        |  `collect_cnt_1d` BIGINT,
        |  `collect_unt_1d` BIGINT,
        |  `collect_cnt_7d` BIGINT,
        |  `collect_unt_7d` BIGINT,
        |  `collect_cnt_28d` BIGINT,
        |  `collect_unt_28d` BIGINT,
        |  `comment_cnt_1d` BIGINT,
        |  `comment_unt_1d` BIGINT,
        |  `comment_cnt_7d` BIGINT,
        |  `comment_unt_7d` BIGINT,
        |  `comment_cnt_28d` BIGINT,
        |  `comment_unt_28d` BIGINT,
        |  `share_cnt_1d` BIGINT,
        |  `share_unt_1d` BIGINT,
        |  `share_cnt_7d` BIGINT,
        |  `share_unt_7d` BIGINT,
        |  `share_cnt_28d` BIGINT,
        |  `share_unt_28d` BIGINT,
        |  `quality_score` DOUBLE,
        |  `hot_score` DOUBLE,
        |  `updated_playlist_type` STRING,
        |  `is_playlist_expert` BIGINT,
        |  `playlist_expert_rank` STRING,
        |  `ts_song_num` BIGINT,
        |  `triable_ts_song_num` BIGINT,
        |  `copyrighted_song_num` BIGINT,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_song_list_dd` (
        |  `playlist_id` STRING,
        |  `playlist_name` STRING,
        |  `playlist_user_id` STRING,
        |  `song_id` STRING,
        |  `create_time` STRING,
        |  `singer_artist_id_list` STRING,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin
    )
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `ads_itm_pgc_song_muse_tag_dd` (
        |  `song_id` STRING,
        |  `song_name` STRING,
        |  `album_id` STRING,
        |  `album_name` STRING,
        |  `album_artist_id` STRING,
        |  `album_artist_name` STRING,
        |  `singer_artist_id_list` STRING,
        |  `singer_artist_name_list` STRING,
        |  `writer_artist_id_list` STRING,
        |  `writer_artist_name_list` STRING,
        |  `composer_artist_id_list` STRING,
        |  `composer_artist_name_list` STRING,
        |  `is_musician_singer` STRING,
        |  `cover_type` STRING,
        |  `area_name` STRING,
        |  `standard_language` STRING,
        |  `dialect_language` STRING,
        |  `online_status` STRING,
        |  `first_online_date` STRING,
        |  `last_online_date` STRING,
        |  `album_publish_date` STRING,
        |  `genre_tag_id` STRING,
        |  `genre_tag_name` STRING,
        |  `genre_tag_id_list` STRING,
        |  `genre_tag_name_list` STRING,
        |  `wyscore` DOUBLE,
        |  `dyscore` DOUBLE,
        |  `synthesize_score` DOUBLE,
        |  `bamboo_score` DOUBLE,
        |  `lyric_score` DOUBLE,
        |  `song_score` DOUBLE,
        |  `sing_score` DOUBLE,
        |  `record_auth_status` STRING,
        |  `record_auth_company_name_list` STRING,
        |  `record_ex_start_date` STRING,
        |  `record_ex_end_date` STRING,
        |  `record_permanent_tag` STRING,
        |  `source_type` STRING,
        |  `record_officer_list` STRING,
        |  `record_group_list` STRING,
        |  `lyrics_auth_status` STRING,
        |  `lyrics_auth_company_name_list` STRING,
        |  `lyrics_ex_start_date` STRING,
        |  `lyrics_ex_end_date` STRING,
        |  `composition_auth_status` STRING,
        |  `composition_auth_company_name_list` STRING,
        |  `composition_ex_start_date` STRING,
        |  `composition_ex_end_date` STRING,
        |  `quality_level` STRING,
        |  `quality_score` BIGINT,
        |  `play_cnt_1d` BIGINT,
        |  `play_unt_1d` BIGINT,
        |  `play_cnt_7d` BIGINT,
        |  `play_cnt_28d` BIGINT,
        |  `play_duration_1d` DOUBLE,
        |  `effective_play_duration_1d` DOUBLE,
        |  `effective_play_cnt_1d` BIGINT,
        |  `effective_play_unt_1d` BIGINT,
        |  `effective_play_cnt_7d` BIGINT,
        |  `effective_play_unt_7d` BIGINT,
        |  `effective_play_cnt_28d` BIGINT,
        |  `effective_play_unt_28d` BIGINT,
        |  `full_play_cnt_1d` BIGINT,
        |  `full_play_unt_1d` BIGINT,
        |  `full_play_cnt_7d` BIGINT,
        |  `full_play_cnt_28d` BIGINT,
        |  `repeat_effective_play_unt_1d` BIGINT,
        |  `repeat_effective_play_unt_7d` BIGINT,
        |  `repeat_effective_play_unt_28d` BIGINT,
        |  `download_cnt_1d` BIGINT,
        |  `download_unt_1d` BIGINT,
        |  `download_cnt_7d` BIGINT,
        |  `download_cnt_28d` BIGINT,
        |  `collect_cnt_1d` BIGINT,
        |  `collect_unt_1d` BIGINT,
        |  `collect_cnt_7d` BIGINT,
        |  `collect_unt_7d` BIGINT,
        |  `collect_cnt_28d` BIGINT,
        |  `collect_unt_28d` BIGINT,
        |  `like_cnt_1d` BIGINT,
        |  `like_unt_1d` BIGINT,
        |  `like_cnt_7d` BIGINT,
        |  `like_cnt_28d` BIGINT,
        |  `comment_cnt_1d` BIGINT,
        |  `comment_unt_1d` BIGINT,
        |  `comment_cnt_7d` BIGINT,
        |  `comment_cnt_28d` BIGINT,
        |  `share_cnt_1d` BIGINT,
        |  `share_unt_1d` BIGINT,
        |  `share_cnt_7d` BIGINT,
        |  `share_cnt_28d` BIGINT,
        |  `effective_play_cnt_1d_rate` DOUBLE,
        |  `full_play_cnt_1d_rate` DOUBLE,
        |  `full_play_cnt_7d_rate` DOUBLE,
        |  `repeat_effective_play_unt_1d_rate` DOUBLE,
        |  `repeat_effective_play_unt_7d_rate` DOUBLE,
        |  `like_effective_cnt_1d_rate` DOUBLE,
        |  `like_effective_cnt_7d_rate` DOUBLE,
        |  `like_cnt_1d_rate` DOUBLE,
        |  `like_cnt_7d_rate` DOUBLE,
        |  `play_cnt_1d_control` BIGINT,
        |  `play_cnt_7d_control` BIGINT,
        |  `full_play_cnt_1d_control` BIGINT,
        |  `full_play_cnt_7d_control` BIGINT,
        |  `collect_cnt_1d_control` BIGINT,
        |  `collect_cnt_7d_control` BIGINT,
        |  `like_cnt_7d_control` BIGINT,
        |  `effective_play_cnt_1d_initiative` BIGINT,
        |  `effective_play_cnt_1d_control` BIGINT,
        |  `null_effective_play_cnt_1d` BIGINT,
        |  `controlled_effective_play_l1d_rate` DOUBLE,
        |  `controlled_collect_roi_l1d` DOUBLE,
        |  `controlled_collect_roi_l7d` DOUBLE,
        |  `controlled_full_play_roi_l1d` DOUBLE,
        |  `controlled_full_play_roi_l7d` DOUBLE,
        |  `controlled_like_roi_l1d` DOUBLE,
        |  `controlled_like_roi_l7d` DOUBLE,
        |  `null_effective_play_cnt_7d` BIGINT,
        |  `effective_play_cnt_1d_initiative_rate` DOUBLE,
        |  `effective_play_cnt_1d_null_rate` DOUBLE,
        |  `first_play_new_unt_1d` BIGINT,
        |  `effective_play_cnt_std` BIGINT,
        |  `download_cnt_std` BIGINT,
        |  `like_cnt_std` BIGINT,
        |  `collect_cnt_std` BIGINT,
        |  `comment_cnt_std` BIGINT,
        |  `share_cnt_std` BIGINT,
        |  `fist_play_new_user_total` BIGINT,
        |  `max_effective_play_cnt_7d` DOUBLE,
        |  `max_effective_play_cnt_7d_reach_date` STRING,
        |  `max_first_play_new_unt_1d` BIGINT,
        |  `max_first_play_new_unt_1d_reach_date` STRING,
        |  `bamboo_level` STRING,
        |  `bamboo_level_reach_date` STRING,
        |  `bamboo_ss_reach_date` STRING,
        |  `bamboo_s_reach_date` STRING,
        |  `bamboo_a_reach_date` STRING,
        |  `bamboo_b_reach_date` STRING,
        |  `first_play_new_unt_30d` BIGINT,
        |  `max_first_play_new_unt_30d` BIGINT,
        |  `max_first_play_new_unt_30d_reach_date` STRING,
        |  `play_rank` STRING,
        |  `play_reach_day` STRING,
        |  `reach_ss_date` STRING,
        |  `reach_s_date` STRING,
        |  `reach_a_date` STRING,
        |  `reach_b_date` STRING,
        |  `reach_c_date` STRING,
        |  `reach_d_date` STRING,
        |  `reach_e_date` STRING,
        |  `reach_f_date` STRING,
        |  `reach_g_date` STRING,
        |  `reach_h_date` STRING,
        |  `current_rank_days` BIGINT,
        |  `rank_days_median` DOUBLE,
        |  `effective_play_cnt_1d_max_l7d` BIGINT,
        |  `effective_play_cnt_1d_min_l7d` BIGINT,
        |  `effective_play_cnt_1d_rate_avg_l7d` DOUBLE,
        |  `effective_play_cnt_1d_rate_avg_l28d` DOUBLE,
        |  `full_play_cnt_1d_rate_avg_l7d` DOUBLE,
        |  `full_play_cnt_1d_rate_avg_l28d` DOUBLE,
        |  `collect_cnt_1d_rate_avg_l7d` DOUBLE,
        |  `like_cnt_1d_rate_avg_l7d` DOUBLE,
        |  `download_cnt_1d_rate_avg_l7d` DOUBLE,
        |  `comment_cnt_1d_rate_avg_l7d` DOUBLE,
        |  `share_cnt_1d_rate_avg_l7d` DOUBLE,
        |  `search_play_cnt_1d` BIGINT,
        |  `single_play_cnt_1d` BIGINT,
        |  `first_play_cnt_1d` BIGINT,
        |  `social_cnt_1d` BIGINT,
        |  `search_rate_1d` DOUBLE,
        |  `single_rate_1d` DOUBLE,
        |  `first_rate_1d` DOUBLE,
        |  `end_play_rate_1d` DOUBLE,
        |  `social_rate_1d` DOUBLE,
        |  `initiative_rate_1d` DOUBLE,
        |  `initiative_social_rate_1d` DOUBLE,
        |  `search_play_cnt_1d_avg_l7d` DOUBLE,
        |  `single_play_cnt_1d_avg_l7d` DOUBLE,
        |  `first_play_cnt_1d_avg_l7d` DOUBLE,
        |  `search_play_cnt_1d_rate_l7d` DOUBLE,
        |  `single_play_cnt_1d_rate_l7d` DOUBLE,
        |  `first_play_cnt_1d_rate_l7d` DOUBLE,
        |  `social_cnt_1d_rate_l7d` DOUBLE,
        |  `initiative_rate_l7d` DOUBLE,
        |  `initiative_social_rate_l7d` DOUBLE,
        |  `social_cnt_1d_avg_l7d` DOUBLE,
        |  `initiative_rate_avg_l7d` DOUBLE,
        |  `mlog_num` BIGINT,
        |  `mlog_play_cnt_std` BIGINT,
        |  `mlog_play_cnt_max` BIGINT,
        |  `mlog_link` STRING,
        |  `mlog_content` STRING,
        |  `effective_play_song_share_land_cnt_1d` BIGINT,
        |  `web_play_ratio` DOUBLE,
        |  `app_play_ratio` DOUBLE,
        |  `musician_level` STRING,
        |  `singer_song_num` BIGINT,
        |  `singer_aplus_song_num` BIGINT,
        |  `writer_song_num` BIGINT,
        |  `writer_aplus_song_num` BIGINT,
        |  `composer_song_num` BIGINT,
        |  `composer_aplus_song_num` BIGINT,
        |  `new2` BIGINT,
        |  `active2` BIGINT,
        |  `recall2` BIGINT,
        |  `buy_vip_user` BIGINT,
        |  `soar_best_rank` STRING,
        |  `original_best_rank` STRING,
        |  `new_best_rank` STRING,
        |  `hot_best_rank` STRING,
        |  `popularity_best_rank` STRING,
        |  `soar_last_rank` STRING,
        |  `original_last_rank` STRING,
        |  `new_last_rank` STRING,
        |  `hot_last_rank` STRING,
        |  `popularity_last_rank` STRING,
        |  `soaring_rank` BIGINT,
        |  `original_rank` BIGINT,
        |  `newsong_rank` BIGINT,
        |  `hotsong_rank` BIGINT,
        |  `popularity_rank` BIGINT,
        |  `effective_play_cnt_1d_avg_l7d` DOUBLE,
        |  `effective_play_cnt_1d_initiative_avg_l7d` DOUBLE,
        |  `effective_play_cnt_1d_control_avg_l7d` DOUBLE,
        |  `effective_play_cnt_1d_null_avg_l7d` DOUBLE,
        |  `full_play_cnt_1d_avg_l7d` DOUBLE,
        |  `play_cnt_1d_avg_l7d` DOUBLE,
        |  `effective_play_cnt_1d_avg_l28d` DOUBLE,
        |  `full_play_cnt_1d_avg_l28d` DOUBLE,
        |  `play_cnt_1d_avg_l28d` DOUBLE,
        |  `effective_play_cnt_1d_avg_l7d_rank_rate` DOUBLE,
        |  `effective_play_pv_l1d_rank_rate` DOUBLE,
        |  `controlled_effective_play_l7d_avg_rate` DOUBLE,
        |  `effective_play_cnt_l7d_avg_initiative_alg_rate` DOUBLE,
        |  `effective_play_cnt_1d_null_avg_l7d_rate` DOUBLE,
        |  `play_cnt_1d_inc_ratio_l7d` DOUBLE,
        |  `play_cnt_1d_inc_ratio_l1d` DOUBLE,
        |  `effective_play_cnt_1d_inc_l7d` DOUBLE,
        |  `effective_play_cnt_1d_inc_l1d` DOUBLE,
        |  `effective_play_cnt_1d_rate_inc_l7d` DOUBLE,
        |  `effective_play_cnt_1d_inc_ratio_l7d` DOUBLE,
        |  `effective_play_cnt_1d_inc_ratio_l1d` DOUBLE,
        |  `effective_play_cnt_1d_rate_inc_ratio_l7d` DOUBLE,
        |  `full_play_cnt_1d_rate_inc_l7d` DOUBLE,
        |  `full_play_cnt_1d_rate_inc_ratio_l7d` DOUBLE,
        |  `full_play_cnt_7d_rate_inc_ratio_l7d` DOUBLE,
        |  `full_play_cnt_1d_rate_inc_ratio_l1d` DOUBLE,
        |  `repeat_effective_play_unt_1d_rate_inc_l7d` DOUBLE,
        |  `repeat_effective_play_unt_1d_rate_inc_ratio_l7d` DOUBLE,
        |  `repeat_effective_play_unt_7d_rate_inc_ratio_l7d` DOUBLE,
        |  `repeat_effective_play_unt_1d_rate_inc_ratio_l1d` DOUBLE,
        |  `repeat_effective_play_unt_7d_rate_inc_ratio_l1d` DOUBLE,
        |  `like_effective_cnt_1d_rate_inc_ratio_l7d` DOUBLE,
        |  `like_effective_cnt_7d_rate_inc_ratio_l7d` DOUBLE,
        |  `like_effective_cnt_1d_rate_inc_ratio_l1d` DOUBLE,
        |  `like_effective_cnt_7d_rate_inc_ratio_l1d` DOUBLE,
        |  `like_cnt_1d_rate_inc_ratio_l7d` DOUBLE,
        |  `like_cnt_7d_rate_inc_ratio_l7d` DOUBLE,
        |  `like_cnt_1d_rate_inc_ratio_l1d` DOUBLE,
        |  `like_cnt_7d_rate_inc_ratio_l1d` DOUBLE,
        |  `effective_play_cnt_7d_avg_inc_l7d` DOUBLE,
        |  `effective_play_cnt_7d_avg_inc_ratio_l7d` DOUBLE,
        |  `full_play_cnt_1d_rate_avg_l7d_ratio_l7d` DOUBLE,
        |  `collect_cnt_1d_rate_avg_l7d_ratio_l7d` DOUBLE,
        |  `like_cnt_1d_rate_avg_l7d_ratio_l7d` DOUBLE,
        |  `comment_cnt_1d_rate_avg_l7d_ratio_l7d` DOUBLE,
        |  `share_cnt_1d_rate_avg_l7d_ratio_l7d` DOUBLE,
        |  `effective_play_cnt_1d_avg_l7d_ratio_l1d` DOUBLE,
        |  `initiative_rate_avg_l7d_ratio_l7d` DOUBLE,
        |  `effective_play_song_share_land_cnt_1d_ratio_l1d` DOUBLE,
        |  `effective_play_song_share_land_cnt_1d_diff` BIGINT,
        |  `controlled_effective_play_avg_rate_l7d_ratio_l1d` DOUBLE,
        |  `controlled_effective_play_avg_rate_l7d_ratio_l7d` DOUBLE,
        |  `effective_play_cnt_initiative_alg_rate_avg_l7d_ratio_l1d` DOUBLE,
        |  `effective_play_cnt_initiative_alg_rate_avg_l7d_ratio_l7d` DOUBLE,
        |  `effective_play_cnt_null_rate_avg_l7d_ratio_l1d` DOUBLE,
        |  `effective_play_cnt_null_rate_avg_l7d_ratio_l7d` DOUBLE,
        |  `effective_play_cnt_1d_initiative_avg_l7d_ratio_l7d` DOUBLE,
        |  `controlled_collect_roi_l7d_ratio_l7d` DOUBLE,
        |  `controlled_like_roi_l7d_ratio_l1d` DOUBLE,
        |  `controlled_like_roi_l7d_ratio_l7d` DOUBLE,
        |  `controlled_full_play_roi_l7d_ratio_l1d` DOUBLE,
        |  `controlled_full_play_roi_l7d_ratio_l7d` DOUBLE,
        |  `controlled_effective_play_rate_l1d_ratio_l1d` DOUBLE,
        |  `controlled_effective_play_rate_l1d_ratio_l7d` DOUBLE,
        |  `controlled_collect_roi_l1d_ratio_l1d` DOUBLE,
        |  `effective_play_cnt_1d_initiative_rate_ratio_l1d` DOUBLE,
        |  `effective_play_cnt_1d_initiative_rate_ratio_l7d` DOUBLE,
        |  `effective_play_cnt_1d_null_rate_ratio_l1d` DOUBLE,
        |  `effective_play_cnt_1d_null_rate_ratio_l7d` DOUBLE,
        |  `effective_play_cnt_1d_initiative_ratio_l1d` DOUBLE,
        |  `inc_7d` DOUBLE,
        |  `inc_1d` DOUBLE,
        |  `ratio_7d` DOUBLE,
        |  `ratio_1d` DOUBLE,
        |  `duration` BIGINT,
        |  `avg_score` DOUBLE,
        |  `is_presale` STRING,
        |  `play_level` STRING,
        |  `play_level_1d` STRING,
        |  `bambo_level_1d` STRING,
        |  `effective_play_cnt_1d_avg_l3d` DOUBLE,
        |  `full_play_cnt_1d_avg_l3d` DOUBLE,
        |  `effective_play_cnt_1d_avg_l3d_ratio_l3d` DOUBLE,
        |  `effective_play_cnt_1d_avg_l3d_ratio_l7d` DOUBLE,
        |  `play_fake_cnt_1d` BIGINT,
        |  `effective_play_fake_cnt_1d` BIGINT,
        |  `like_fake_cnt_1d` BIGINT,
        |  `download_fake_cnt_1d` BIGINT,
        |  `collect_fake_cnt_1d` BIGINT,
        |  `comment_fake_cnt_1d` BIGINT,
        |  `share_fake_cnt_1d` BIGINT,
        |  `incre_ratio` DOUBLE,
        |  `spam_ratio` DOUBLE,
        |  `tag` BIGINT,
        |  `play_unt_7d` BIGINT,
        |  `play_unt_28d` BIGINT,
        |  `top_source_1` STRING,
        |  `top_source_1_source_ratio` DOUBLE,
        |  `top_source_1_effective_play_cnt_1d_diff` BIGINT,
        |  `top_source_2` STRING,
        |  `top_source_2_source_ratio` DOUBLE,
        |  `top_source_2_effective_play_cnt_1d_diff` BIGINT,
        |  `top_source_3` STRING,
        |  `top_source_3_source_ratio` DOUBLE,
        |  `top_source_3_effective_play_cnt_1d_diff` BIGINT,
        |  `is_promote` STRING,
        |  `record_contract_serial_list` STRING,
        |  `effective_play_cnt_1d_initiative_avg_l7d_ratio_l1d` DOUBLE,
        |  `record_studio_name_list` STRING,
        |  `recall_unt_1d` BIGINT,
        |  `active_unt_1d` BIGINT,
        |  `max_recall_unt_1d` BIGINT,
        |  `max_active_unt_1d` BIGINT,
        |  `max_recall_unt_1d_reach_date` STRING,
        |  `max_active_unt_1d_reach_date` STRING,
        |  `like_effective_cnt_1d_new_rate` DOUBLE,
        |  `like_cnt_1d_new_rate` DOUBLE,
        |  `like_effective_cnt_7d_new_rate` DOUBLE,
        |  `like_cnt_7d_new_rate` DOUBLE,
        |  `first_play_recall_unt_1d` BIGINT,
        |  `first_play_retain_unt_1d` BIGINT,
        |  `max_first_play_recall_unt_1d` BIGINT,
        |  `max_first_play_retain_unt_1d` BIGINT,
        |  `max_first_play_recall_unt_1d_reach_date` STRING,
        |  `max_first_play_retain_unt_1d_reach_date` STRING,
        |  `follow_num` BIGINT,
        |  `effective_play_cnt_2d` BIGINT,
        |  `effective_play_cnt_3d` BIGINT,
        |  `effective_play_cnt_4d` BIGINT,
        |  `effective_play_cnt_5d` BIGINT,
        |  `effective_play_cnt_6d` BIGINT,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_song_tag_summary` (
        |  `playlist_id` STRING,
        |  `song_id` STRING,
        |  `play_cnt_1d` BIGINT,
        |  `effective_play_cnt_1d` BIGINT,
        |  `effective_play_duration_1d` DOUBLE,
        |  `full_play_cnt_1d` BIGINT,
        |  `full_play_duration_1d` DOUBLE,
        |  `like_cnt_1d` BIGINT,
        |  `non_author_play_cnt_1d` BIGINT,
        |  `non_author_effective_play_cnt_1d` BIGINT,
        |  `non_author_effective_play_duration_1d` DOUBLE,
        |  `non_author_full_play_cnt_1d` BIGINT,
        |  `non_author_full_play_duration_1d` DOUBLE,
        |  `non_author_like_cnt_1d` BIGINT,
        |  `play_cnt_7d` BIGINT,
        |  `effective_play_cnt_7d` BIGINT,
        |  `effective_play_duration_7d` DOUBLE,
        |  `full_play_cnt_7d` BIGINT,
        |  `full_play_duration_7d` DOUBLE,
        |  `like_cnt_7d` BIGINT,
        |  `non_author_play_cnt_7d` BIGINT,
        |  `non_author_effective_play_cnt_7d` BIGINT,
        |  `non_author_effective_play_duration_7d` DOUBLE,
        |  `non_author_full_play_cnt_7d` BIGINT,
        |  `non_author_full_play_duration_7d` DOUBLE,
        |  `non_author_like_cnt_7d` BIGINT,
        |  `play_cnt_28d` BIGINT,
        |  `effective_play_cnt_28d` BIGINT,
        |  `effective_play_duration_28d` DOUBLE,
        |  `full_play_cnt_28d` BIGINT,
        |  `full_play_duration_28d` DOUBLE,
        |  `like_cnt_28d` BIGINT,
        |  `non_author_play_cnt_28d` BIGINT,
        |  `non_author_effective_play_cnt_28d` BIGINT,
        |  `non_author_effective_play_duration_28d` DOUBLE,
        |  `non_author_full_play_cnt_28d` BIGINT,
        |  `non_author_full_play_duration_28d` DOUBLE,
        |  `non_author_like_cnt_28d` BIGINT,
        |  `effective_play_cnt_1d_rate` DOUBLE,
        |  `effective_play_cnt_7d_rate` DOUBLE,
        |  `effective_play_cnt_28d_rate` DOUBLE,
        |  `full_play_cnt_1d_rate` DOUBLE,
        |  `full_play_cnt_7d_rate` DOUBLE,
        |  `full_play_cnt_28d_rate` DOUBLE,
        |  `like_cnt_1d_rate` DOUBLE,
        |  `like_cnt_7d_rate` DOUBLE,
        |  `like_cnt_28d_rate` DOUBLE,
        |  `non_author_effective_play_cnt_1d_rate` DOUBLE,
        |  `non_author_effective_play_cnt_7d_rate` DOUBLE,
        |  `non_author_effective_play_cnt_28d_rate` DOUBLE,
        |  `non_author_full_play_cnt_1d_rate` DOUBLE,
        |  `non_author_full_play_cnt_7d_rate` DOUBLE,
        |  `non_author_full_play_cnt_28d_rate` DOUBLE,
        |  `non_author_like_cnt_1d_rate` DOUBLE,
        |  `non_author_like_cnt_7d_rate` DOUBLE,
        |  `non_author_like_cnt_28d_rate` DOUBLE,
        |  `non_author_initiative_effective_play_cnt_7d` BIGINT,
        |  `non_author_initiative_effective_play_duration_7d` DOUBLE,
        |  `non_author_alg_effective_play_cnt_7d` BIGINT,
        |  `non_author_alg_effective_play_duration_7d` DOUBLE,
        |  `non_author_operate_effective_play_cnt_7d` BIGINT,
        |  `non_author_operate_effective_play_duration_7d` DOUBLE,
        |  `non_author_other_effective_play_cnt_7d` BIGINT,
        |  `non_author_other_effective_play_duration_7d` DOUBLE,
        |  `non_author_initiative_effective_play_cnt_28d` BIGINT,
        |  `non_author_initiative_effective_play_duration_28d` DOUBLE,
        |  `non_author_alg_effective_play_cnt_28d` BIGINT,
        |  `non_author_alg_effective_play_duration_28d` DOUBLE,
        |  `non_author_operate_effective_play_cnt_28d` BIGINT,
        |  `non_author_operate_effective_play_duration_28d` DOUBLE,
        |  `non_author_other_effective_play_cnt_28d` BIGINT,
        |  `non_author_other_effective_play_duration_28d` DOUBLE,
        |  `non_author_initiative_effective_play_cnt_1d` BIGINT,
        |  `non_author_initiative_effective_play_duration_1d` DOUBLE,
        |  `non_author_alg_effective_play_cnt_1d` BIGINT,
        |  `non_author_alg_effective_play_duration_1d` DOUBLE,
        |  `non_author_operate_effective_play_cnt_1d` BIGINT,
        |  `non_author_operate_effective_play_duration_1d` DOUBLE,
        |  `non_author_other_effective_play_cnt_1d` BIGINT,
        |  `non_author_other_effective_play_duration_1d` DOUBLE,
        |  `non_author_play_unt_1d` BIGINT,
        |  `non_author_effective_play_unt_1d` BIGINT,
        |  `non_author_full_play_unt_1d` BIGINT,
        |  `non_author_like_unt_1d` BIGINT,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_dau_vs_mau_di` (
        |  `playlist_id` STRING,
        |  `mau` BIGINT,
        |  `dau_sum` BIGINT,
        |  `dau_vs_mau` DOUBLE,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_song_dist_28d` (
        |  `playlist_id` STRING,
        |  `non_author_effective_play_cnt_28d` BIGINT,
        |  `top5_non_author_effective_play_cnt_28d` BIGINT,
        |  `top25_non_author_effective_play_cnt_28d` BIGINT,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_user_dist_28d` (
        |  `playlist_id` STRING,
        |  `non_author_effective_play_cnt_28d` BIGINT,
        |  `top5_non_author_effective_play_cnt_28d` BIGINT,
        |  `top25_non_author_effective_play_cnt_28d` BIGINT,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_app_song_dist_7d` (
        |  `playlist_id` STRING,
        |  `effective_play_cnt_7d` BIGINT,
        |  `top5_effective_play_cnt_7d` BIGINT,
        |  `top25_effective_play_cnt_7d` BIGINT,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_creator_tag_dd` (
        |  `playlist_id` STRING,
        |  `user_id` STRING,
        |  `effective_play_song_num_28d` BIGINT,
        |  `effective_play_cnt_28d` BIGINT,
        |  `effective_play_duration_28d` DOUBLE,
        |  `db_collect_playlist_num_std` BIGINT,
        |  `db_followed_unt_std` BIGINT,
        |  `collected_playlist_num` BIGINT,
        |  `collected_playlist_cnt` BIGINT,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_log_tag_28d` (
        |  `playlist_id` STRING,
        |  `exp_cnt_1d` BIGINT,
        |  `clk_cnt_1d` BIGINT,
        |  `initiative_exp_cnt_1d` BIGINT,
        |  `initiative_clk_cnt_1d` BIGINT,
        |  `alg_exp_cnt_1d` BIGINT,
        |  `alg_clk_cnt_1d` BIGINT,
        |  `operate_exp_cnt_1d` BIGINT,
        |  `operate_clk_cnt_1d` BIGINT,
        |  `other_exp_cnt_1d` BIGINT,
        |  `other_clk_cnt_1d` BIGINT,
        |  `exp_unt_1d` BIGINT,
        |  `clk_unt_1d` BIGINT,
        |  `initiative_exp_unt_1d` BIGINT,
        |  `initiative_clk_unt_1d` BIGINT,
        |  `alg_exp_unt_1d` BIGINT,
        |  `alg_clk_unt_1d` BIGINT,
        |  `operate_exp_unt_1d` BIGINT,
        |  `operate_clk_unt_1d` BIGINT,
        |  `other_exp_unt_1d` BIGINT,
        |  `other_clk_unt_1d` BIGINT,
        |  `exp_cnt_7d` BIGINT,
        |  `clk_cnt_7d` BIGINT,
        |  `initiative_exp_cnt_7d` BIGINT,
        |  `initiative_clk_cnt_7d` BIGINT,
        |  `alg_exp_cnt_7d` BIGINT,
        |  `alg_clk_cnt_7d` BIGINT,
        |  `operate_exp_cnt_7d` BIGINT,
        |  `operate_clk_cnt_7d` BIGINT,
        |  `other_exp_cnt_7d` BIGINT,
        |  `other_clk_cnt_7d` BIGINT,
        |  `exp_unt_7d` BIGINT,
        |  `clk_unt_7d` BIGINT,
        |  `initiative_exp_unt_7d` BIGINT,
        |  `initiative_clk_unt_7d` BIGINT,
        |  `alg_exp_unt_7d` BIGINT,
        |  `alg_clk_unt_7d` BIGINT,
        |  `operate_exp_unt_7d` BIGINT,
        |  `operate_clk_unt_7d` BIGINT,
        |  `other_exp_unt_7d` BIGINT,
        |  `other_clk_unt_7d` BIGINT,
        |  `exp_cnt_28d` BIGINT,
        |  `clk_cnt_28d` BIGINT,
        |  `initiative_exp_cnt_28d` BIGINT,
        |  `initiative_clk_cnt_28d` BIGINT,
        |  `alg_exp_cnt_28d` BIGINT,
        |  `alg_clk_cnt_28d` BIGINT,
        |  `operate_exp_cnt_28d` BIGINT,
        |  `operate_clk_cnt_28d` BIGINT,
        |  `other_exp_cnt_28d` BIGINT,
        |  `other_clk_cnt_28d` BIGINT,
        |  `exp_unt_28d` BIGINT,
        |  `clk_unt_28d` BIGINT,
        |  `initiative_exp_unt_28d` BIGINT,
        |  `initiative_clk_unt_28d` BIGINT,
        |  `alg_exp_unt_28d` BIGINT,
        |  `alg_clk_unt_28d` BIGINT,
        |  `operate_exp_unt_28d` BIGINT,
        |  `operate_clk_unt_28d` BIGINT,
        |  `other_exp_unt_28d` BIGINT,
        |  `other_clk_unt_28d` BIGINT,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `user_first_play_day` (
        |  `rid` BIGINT,
        |  `resource_type` STRING,
        |  `resource_id` BIGINT,
        |  `source_type` STRING,
        |  `source_id` BIGINT,
        |  `logtime` BIGINT,
        |  `os` STRING,
        |  `first_log_time` BIGINT,
        |  `ban` INT,
        |  `attribute` INT,
        |  `play_duration` BIGINT,
        |  `userid` BIGINT,
        |  `refer` STRING,
        |  `time_valid` INT,
        |  `is_effective` INT,
        |  `is_fake` INT,
        |  `end_type` STRING,
        |  `second_last_log_date` STRING,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_user_tag_summary` (
        |  `playlist_id` STRING,
        |  `user_id` STRING,
        |  `is_playlist_author` BIGINT,
        |  `song_play_cnt_1d` BIGINT,
        |  `song_effective_play_cnt_1d` BIGINT,
        |  `song_effective_play_duration_1d` DOUBLE,
        |  `song_full_play_cnt_1d` BIGINT,
        |  `song_full_play_duration_1d` DOUBLE,
        |  `song_like_cnt_1d` BIGINT,
        |  `play_song_num_1d` BIGINT,
        |  `effective_play_song_num_1d` BIGINT,
        |  `full_play_song_num_1d` BIGINT,
        |  `like_song_num_1d` BIGINT,
        |  `song_play_cnt_7d` BIGINT,
        |  `song_effective_play_cnt_7d` BIGINT,
        |  `song_effective_play_duration_7d` DOUBLE,
        |  `song_full_play_cnt_7d` BIGINT,
        |  `song_full_play_duration_7d` DOUBLE,
        |  `song_like_cnt_7d` BIGINT,
        |  `song_play_cnt_28d` BIGINT,
        |  `song_effective_play_cnt_28d` BIGINT,
        |  `song_effective_play_duration_28d` DOUBLE,
        |  `song_full_play_cnt_28d` BIGINT,
        |  `song_full_play_duration_28d` DOUBLE,
        |  `song_like_cnt_28d` BIGINT,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE IF NOT EXISTS `tmp_ads_itm_muse_playlist_main_genre_tag_dd` (
        |  `playlist_id` STRING,
        |  `standard_language` STRING,
        |  `genre_tag_name` STRING,
        |  `pct` DOUBLE,
        |  `dt` STRING)
        |USING parquet
        |PARTITIONED BY (dt);
        |""".stripMargin)
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_tag_dd")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_song_list_dd")
      sql("DROP TABLE IF EXISTS ads_itm_pgc_song_muse_tag_dd")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_song_tag_summary")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_dau_vs_mau_di")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_song_dist_28d")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_user_dist_28d")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_app_song_dist_7d")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_creator_tag_dd")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_log_tag_28d")
      sql("DROP TABLE IF EXISTS user_first_play_day")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_user_tag_summary")
      sql("DROP TABLE IF EXISTS tmp_ads_itm_muse_playlist_main_genre_tag_dd")
    } finally {
      spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)
      spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY)
      super.afterAll()
    }
  }

  test("dpp and aqe cost lots of time in InsertAdaptiveSparkPlan#buildSubqueryMap") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {


      RuleExecutor.resetMetrics()
      sql(sql1 + sql2).collect()
      // noinspection ScalaStyle
      println(RuleExecutor.dumpTimeSpent())
    }
  }

  // noinspection ScalaStyle
  val sql1 =
    """
      |select  a.playlist_id
      |        ,a.playlist_name
      |        ,a.playlist_type
      |        ,a.user_id
      |        ,a.nick_name
      |        ,a.create_date
      |        ,a.create_period
      |        ,a.create_playlist_duration
      |        ,a.description
      |        ,a.is_high_quality
      |        ,a.cover_status
      |        ,a.cover_id
      |        ,a.song_num
      |        ,a.online_song_num
      |        ,a.cloud_song_num
      |        ,a.play_num
      |        ,a.display_play_num
      |        ,a.collect_num
      |        ,a.share_num
      |        ,a.comment_num
      |        ,a.fee_song_num
      |        ,a.fee_level
      |        ,a.user_tag_list
      |        ,case   when a.user_tag_list is not null and trim(a.user_tag_list) <> '' then '1'
      |                else '0'
      |                end as with_user_tag
      |        ,a.language_area
      |        ,a.genre_tag_list
      |        ,a.is_genre_matched
      |        ,a.is_antispam
      |        ,a.is_compliant
      |        ,a.play_cnt_1d
      |        ,a.play_unt_1d
      |        ,a.play_cnt_7d
      |        ,a.play_unt_7d
      |        ,a.play_cnt_28d
      |        ,a.play_unt_28d
      |        ,a.song_effective_play_cnt_1d
      |        ,a.song_effective_play_unt_1d
      |        ,a.song_effective_play_cnt_7d
      |        ,a.song_effective_play_unt_7d
      |        ,a.song_effective_play_cnt_28d
      |        ,a.song_effective_play_unt_28d
      |        ,a.collect_cnt_1d
      |        ,a.collect_unt_1d
      |        ,a.collect_cnt_7d
      |        ,a.collect_unt_7d
      |        ,a.collect_cnt_28d
      |        ,a.collect_unt_28d
      |        ,a.comment_cnt_1d
      |        ,a.comment_unt_1d
      |        ,a.comment_cnt_7d
      |        ,a.comment_unt_7d
      |        ,a.comment_cnt_28d
      |        ,a.comment_unt_28d
      |        ,a.share_cnt_1d
      |        ,a.share_unt_1d
      |        ,a.share_cnt_7d
      |        ,a.share_unt_7d
      |        ,a.share_cnt_28d
      |        ,a.share_unt_28d
      |        ,a.quality_score
      |        ,a.hot_score
      |        ,a.updated_playlist_type
      |        ,a.is_playlist_expert
      |        ,a.playlist_expert_rank
      |        --更新歌曲频次
      |        ,case   when b.create_song_num_1d > 0 then '近1日有新增歌曲'
      |                when b.create_song_num_7d > 0 then '近2-7日有新增歌曲'
      |                when b.create_song_num_28d > 0 then '近8-28日有新增歌曲'
      |                when b.create_song_num_60d > 0 then '近29-60日有新增歌曲'
      |                when b.create_song_num_90d > 0 then '近61-90日有新增歌曲'
      |                when b.create_song_num_180d > 0 then '近91-180日有新增歌曲'
      |                when b.create_song_num_180d = 0 then '近180日无新增歌曲'
      |                when b.last_create_date is null and a.playlist_type = '100' then '每日算法更新'
      |                else null
      |                end as create_song_period
      |        ,nvl(b.create_song_num_28d, 0) as create_song_num_28d
      |        ,nvl(b.create_song_days_28d, 0) as create_song_days_28d
      |        ,nvl(b.create_song_num_60d, 0) as create_song_num_60d
      |        ,nvl(b.create_song_days_60d, 0) as create_song_days_60d
      |        --各等级歌曲数
      |        ,nvl(c.play_rank_a_plus_song_num, 0) as play_rank_a_plus_song_num
      |        ,nvl(c.play_rank_middle_song_num, 0) as play_rank_middle_song_num
      |        ,nvl(c.play_rank_small_song_num, 0) as play_rank_small_song_num
      |        ,nvl(c.play_rank_ssmall_song_num, 0) as play_rank_ssmall_song_num
      |        ,nvl(c.play_rank_ss_song_num, 0) as play_rank_ss_song_num
      |        ,nvl(c.play_rank_s_song_num, 0) as play_rank_s_song_num
      |        ,nvl(c.play_rank_a_song_num, 0) as play_rank_a_song_num
      |        ,nvl(c.play_rank_b_song_num, 0) as play_rank_b_song_num
      |        ,nvl(c.play_rank_c_song_num, 0) as play_rank_c_song_num
      |        ,nvl(c.play_rank_d_song_num, 0) as play_rank_d_song_num
      |        ,nvl(c.play_rank_e_song_num, 0) as play_rank_e_song_num
      |        ,nvl(c.play_rank_f_song_num, 0) as play_rank_f_song_num
      |        ,nvl(c.play_rank_g_song_num, 0) as play_rank_g_song_num
      |        ,nvl(c.play_rank_h_song_num, 0) as play_rank_h_song_num
      |        ,nvl(c.valid_song_num, 0) as valid_song_num --有效歌曲数
      |        ,nvl(c.gray_auth_song_num, 0) as gray_auth_song_num --灰色版权歌曲数
      |        ,nvl(c.gray_auth_song_num, 0) / nvl(a.online_song_num, 0) as gray_auth_song_rate --灰色版权歌曲比例
      |        --剔除作者的听歌行为
      |        ,nvl(d.non_author_play_cnt_7d_avg, 0) as non_author_song_play_cnt_7d_avg
      |        ,nvl(d.non_author_effective_play_cnt_7d_avg, 0) as non_author_song_effective_play_cnt_7d_avg
      |        ,nvl(d.non_author_effective_play_duration_7d_avg, 0) as non_author_song_effective_play_duration_7d_avg
      |        ,nvl(d.non_author_full_play_cnt_7d_avg, 0) as non_author_song_full_play_cnt_7d_avg
      |        ,nvl(d.non_author_full_play_duration_7d_avg, 0) as non_author_song_full_play_duration_7d_avg
      |        ,nvl(d.non_author_like_cnt_7d_avg, 0) as non_author_song_like_cnt_7d_avg
      |        ,nvl(d.non_author_play_cnt_28d_avg, 0) as non_author_song_play_cnt_28d_avg
      |        ,nvl(d.non_author_effective_play_cnt_28d_avg, 0) as non_author_song_effective_play_cnt_28d_avg
      |        ,nvl(d.non_author_effective_play_duration_28d_avg, 0) as non_author_song_effective_play_duration_28d_avg
      |        ,nvl(d.non_author_full_play_cnt_28d_avg, 0) as non_author_song_full_play_cnt_28d_avg
      |        ,nvl(d.non_author_full_play_duration_28d_avg, 0) as non_author_song_full_play_duration_28d_avg
      |        ,nvl(d.non_author_like_cnt_28d_avg, 0) as non_author_song_like_cnt_28d_avg
      |        ,nvl(d.effective_play_cnt_1d_rate_avg, 0) as song_effective_play_cnt_1d_rate_avg
      |        ,nvl(d.effective_play_cnt_7d_rate_avg, 0) as song_effective_play_cnt_7d_rate_avg
      |        ,nvl(d.effective_play_cnt_28d_rate_avg, 0) as song_effective_play_cnt_28d_rate_avg
      |        ,nvl(d.full_play_cnt_1d_rate_avg, 0) as song_full_play_cnt_1d_rate_avg
      |        ,nvl(d.full_play_cnt_7d_rate_avg, 0) as song_full_play_cnt_7d_rate_avg
      |        ,nvl(d.full_play_cnt_28d_rate_avg, 0) as song_full_play_cnt_28d_rate_avg
      |        ,nvl(d.like_cnt_1d_rate_avg, 0) as song_like_cnt_1d_rate_avg
      |        ,nvl(d.like_cnt_7d_rate_avg, 0) as song_like_cnt_7d_rate_avg
      |        ,nvl(d.like_cnt_28d_rate_avg, 0) as song_like_cnt_28d_rate_avg
      |        ,nvl(d.non_author_effective_play_cnt_1d_rate_avg, 0) as non_author_song_effective_play_cnt_1d_rate_avg
      |        ,nvl(d.non_author_effective_play_cnt_7d_rate_avg, 0) as non_author_song_effective_play_cnt_7d_rate_avg
      |        ,nvl(d.non_author_effective_play_cnt_28d_rate_avg, 0) as non_author_song_effective_play_cnt_28d_rate_avg
      |        ,nvl(d.non_author_full_play_cnt_1d_rate_avg, 0) as non_author_song_full_play_cnt_1d_rate_avg
      |        ,nvl(d.non_author_full_play_cnt_7d_rate_avg, 0) as non_author_song_full_play_cnt_7d_rate_avg
      |        ,nvl(d.non_author_full_play_cnt_28d_rate_avg, 0) as non_author_song_full_play_cnt_28d_rate_avg
      |        ,nvl(d.non_author_like_cnt_1d_rate_avg, 0) as non_author_song_like_cnt_1d_rate_avg
      |        ,nvl(d.non_author_like_cnt_7d_rate_avg, 0) as non_author_song_like_cnt_7d_rate_avg
      |        ,nvl(d.non_author_like_cnt_28d_rate_avg, 0) as non_author_song_like_cnt_28d_rate_avg
      |        --歌单复播
      |        ,nvl(e.dau_vs_mau, 0) as dau_vs_mau
      |        --集中度
      |        ,nvl(f.list_top5_song_effective_play_cnt_28d_avg, 0) as list_top5_song_effective_play_cnt_28d_avg
      |        ,nvl(f.list_top25_song_effective_play_cnt_28d_avg, 0) as list_top25_song_effective_play_cnt_28d_avg
      |        ,nvl(f.list_top5_song_effective_play_pct_28d, 0) as list_top5_song_effective_play_pct_28d
      |        ,nvl(f.list_top25_song_effective_play_pct_28d, 0) as list_top25_song_effective_play_pct_28d
      |        ,nvl(g.list_top5_user_effective_play_cnt_28d_avg, 0) as list_top5_user_effective_play_cnt_28d_avg
      |        ,nvl(g.list_top25_user_effective_play_cnt_28d_avg, 0) as list_top25_user_effective_play_cnt_28d_avg
      |        ,nvl(g.list_top5_user_effective_play_pct_28d, 0) as list_top5_user_effective_play_pct_28d
      |        ,nvl(g.list_top25_user_effective_play_pct_28d, 0) as list_top25_user_effective_play_pct_28d
      |        ,nvl(h.app_top5_song_effective_play_cnt_7d_avg, 0) as app_top5_song_effective_play_cnt_7d_avg
      |        ,nvl(h.app_top25_song_effective_play_cnt_7d_avg, 0) as app_top25_song_effective_play_cnt_7d_avg
      |        ,nvl(h.app_top5_song_effective_play_pct_7d, 0) as app_top5_song_effective_play_pct_7d
      |        ,nvl(h.app_top25_song_effective_play_pct_7d, 0) as app_top25_song_effective_play_pct_7d
      |        --歌单创建人信息
      |        ,nvl(i.effective_play_song_num_28d, 0) as creator_effective_play_song_num_28d
      |        ,nvl(i.effective_play_cnt_28d, 0) as creator_effective_play_song_cnt_28d
      |        ,nvl(i.effective_play_duration_28d, 0) as creator_effective_play_song_duration_28d
      |        ,nvl(i.db_collect_playlist_num_std, 0) as creator_collect_playlist_cnt_std
      |        ,nvl(i.db_followed_unt_std, 0) as creator_followed_unt_std
      |        ,nvl(i.collected_playlist_num, 0) as creator_collected_playlist_num
      |        ,nvl(i.collected_playlist_cnt, 0) as creator_collected_playlist_cnt
      |        --各等级歌曲占比
      |        ,nvl(c.play_rank_a_plus_song_num, 0) / nvl(c.total_song_num, 0) as play_rank_a_plus_song_pct
      |        ,nvl(c.play_rank_middle_song_num, 0) / nvl(c.total_song_num, 0) as play_rank_middle_song_pct
      |        ,nvl(c.play_rank_small_song_num, 0) / nvl(c.total_song_num, 0) as play_rank_small_song_pct
      |        ,nvl(c.play_rank_ssmall_song_num, 0) / nvl(c.total_song_num, 0) as play_rank_ssmall_song_pct
      |        --各等级歌曲播放效果
      |        ,nvl(d.non_author_effective_play_song_num_7d, 0) as non_author_effective_play_song_num_7d
      |        ,nvl(d.non_author_effective_play_cnt_7d, 0) as non_author_song_effective_play_cnt_7d
      |        ,nvl(d.non_author_effective_play_duration_7d, 0) as non_author_song_effective_play_duration_7d
      |        ,nvl(d.non_author_effective_play_song_num_28d, 0) as non_author_effective_play_song_num_28d
      |        ,nvl(d.non_author_effective_play_cnt_28d, 0) as non_author_song_effective_play_cnt_28d
      |        ,nvl(d.non_author_effective_play_duration_28d, 0) as non_author_song_effective_play_duration_28d
      |        ,nvl(d.non_author_a_plus_effective_play_song_num_28d, 0) as non_author_a_plus_effective_play_song_num_28d
      |        ,nvl(d.non_author_middle_effective_play_song_num_28d, 0) as non_author_middle_effective_play_song_num_28d
      |        ,nvl(d.non_author_small_effective_play_song_num_28d, 0) as non_author_small_effective_play_song_num_28d
      |        ,nvl(d.non_author_ssmall_effective_play_song_num_28d, 0) as non_author_ssmall_effective_play_song_num_28d
      |        ,nvl(d.non_author_a_plus_effective_play_song_num_28d, 0) / nvl(d.non_author_effective_play_song_num_28d, 0) as non_author_a_plus_effective_play_song_pct_28d
      |        ,nvl(d.non_author_middle_effective_play_song_num_28d, 0) / nvl(d.non_author_effective_play_song_num_28d, 0) as non_author_middle_effective_play_song_pct_28d
      |        ,nvl(d.non_author_small_effective_play_song_num_28d, 0) / nvl(d.non_author_effective_play_song_num_28d, 0) as non_author_small_effective_play_song_pct_28d
      |        ,nvl(d.non_author_ssmall_effective_play_song_num_28d, 0) / nvl(d.non_author_effective_play_song_num_28d, 0) as non_author_ssmall_effective_play_song_pct_28d
      |        ,nvl(d.non_author_a_plus_effective_play_cnt_28d, 0) as non_author_a_plus_song_effective_play_cnt_28d
      |        ,nvl(d.non_author_middle_effective_play_cnt_28d, 0) as non_author_middle_song_effective_play_cnt_28d
      |        ,nvl(d.non_author_small_effective_play_cnt_28d, 0) as non_author_small_song_effective_play_cnt_28d
      |        ,nvl(d.non_author_ssmall_effective_play_cnt_28d, 0) as non_author_ssmall_song_effective_play_cnt_28d
      |        ,nvl(d.non_author_a_plus_effective_play_duration_28d, 0) as non_author_a_plus_song_effective_play_duration_28d
      |        ,nvl(d.non_author_middle_effective_play_duration_28d, 0) as non_author_middle_song_effective_play_duration_28d
      |        ,nvl(d.non_author_small_effective_play_duration_28d, 0) as non_author_small_song_effective_play_duration_28d
      |        ,nvl(d.non_author_ssmall_effective_play_duration_28d, 0) as non_author_ssmall_song_effective_play_duration_28d
      |        --歌单曝光点击
      |        ,nvl(j.exp_cnt_7d, 0) as exp_cnt_7d
      |        ,nvl(j.initiative_exp_cnt_7d, 0) as initiative_exp_cnt_7d
      |        ,nvl(j.alg_exp_cnt_7d, 0) as alg_exp_cnt_7d
      |        ,nvl(j.operate_exp_cnt_7d, 0) as operate_exp_cnt_7d
      |        ,nvl(j.other_exp_cnt_7d, 0) as other_exp_cnt_7d
      |        ,nvl(j.initiative_exp_pct_7d, 0) as initiative_exp_pct_7d
      |        ,nvl(j.alg_exp_pct_7d, 0) as alg_exp_pct_7d
      |        ,nvl(j.operate_exp_pct_7d, 0) as operate_exp_pct_7d
      |        ,nvl(j.other_exp_pct_7d, 0) as other_exp_pct_7d
      |        ,nvl(j.exp_cnt_7d_avg, 0) as exp_cnt_7d_avg
      |        ,nvl(j.initiative_exp_cnt_7d_avg, 0) as initiative_exp_cnt_7d_avg
      |        ,nvl(j.alg_exp_cnt_7d_avg, 0) as alg_exp_cnt_7d_avg
      |        ,nvl(j.operate_exp_cnt_7d_avg, 0) as operate_exp_cnt_7d_avg
      |        ,nvl(j.other_exp_cnt_7d_avg, 0) as other_exp_cnt_7d_avg
      |        ,nvl(j.clk_cnt_7d, 0) as clk_cnt_7d
      |        ,nvl(j.initiative_clk_cnt_7d, 0) as initiative_clk_cnt_7d
      |        ,nvl(j.alg_clk_cnt_7d, 0) as alg_clk_cnt_7d
      |        ,nvl(j.operate_clk_cnt_7d, 0) as operate_clk_cnt_7d
      |        ,nvl(j.other_clk_cnt_7d, 0) as other_clk_cnt_7d
      |        ,nvl(j.initiative_clk_pct_7d, 0) as initiative_clk_pct_7d
      |        ,nvl(j.alg_clk_pct_7d, 0) as alg_clk_pct_7d
      |        ,nvl(j.operate_clk_pct_7d, 0) as operate_clk_pct_7d
      |        ,nvl(j.other_clk_pct_7d, 0) as other_clk_pct_7d
      |        ,nvl(j.clk_cnt_7d_avg, 0) as clk_cnt_7d_avg
      |        ,nvl(j.initiative_clk_cnt_7d_avg, 0) as initiative_clk_cnt_7d_avg
      |        ,nvl(j.alg_clk_cnt_7d_avg, 0) as alg_clk_cnt_7d_avg
      |        ,nvl(j.operate_clk_cnt_7d_avg, 0) as operate_clk_cnt_7d_avg
      |        ,nvl(j.other_clk_cnt_7d_avg, 0) as other_clk_cnt_7d_avg
      |        ,nvl(j.cnt_ctr_7d, 0) as cnt_ctr_7d
      |        ,nvl(j.initiative_cnt_ctr_7d, 0) as initiative_cnt_ctr_7d
      |        ,nvl(j.alg_cnt_ctr_7d, 0) as alg_cnt_ctr_7d
      |        ,nvl(j.operate_cnt_ctr_7d, 0) as operate_cnt_ctr_7d
      |        ,nvl(j.other_cnt_ctr_7d, 0) as other_cnt_ctr_7d
      |        ,nvl(j.exp_cnt_28d, 0) as exp_cnt_28d
      |        ,nvl(j.initiative_exp_cnt_28d, 0) as initiative_exp_cnt_28d
      |        ,nvl(j.alg_exp_cnt_28d, 0) as alg_exp_cnt_28d
      |        ,nvl(j.operate_exp_cnt_28d, 0) as operate_exp_cnt_28d
      |        ,nvl(j.other_exp_cnt_28d, 0) as other_exp_cnt_28d
      |        ,nvl(j.initiative_exp_pct_28d, 0) as initiative_exp_pct_28d
      |        ,nvl(j.alg_exp_pct_28d, 0) as alg_exp_pct_28d
      |        ,nvl(j.operate_exp_pct_28d, 0) as operate_exp_pct_28d
      |        ,nvl(j.other_exp_pct_28d, 0) as other_exp_pct_28d
      |        ,nvl(j.exp_cnt_28d_avg, 0) as exp_cnt_28d_avg
      |        ,nvl(j.initiative_exp_cnt_28d_avg, 0) as initiative_exp_cnt_28d_avg
      |        ,nvl(j.alg_exp_cnt_28d_avg, 0) as alg_exp_cnt_28d_avg
      |        ,nvl(j.operate_exp_cnt_28d_avg, 0) as operate_exp_cnt_28d_avg
      |        ,nvl(j.other_exp_cnt_28d_avg, 0) as other_exp_cnt_28d_avg
      |        ,nvl(j.clk_cnt_28d, 0) as clk_cnt_28d
      |        ,nvl(j.initiative_clk_cnt_28d, 0) as initiative_clk_cnt_28d
      |        ,nvl(j.alg_clk_cnt_28d, 0) as alg_clk_cnt_28d
      |        ,nvl(j.operate_clk_cnt_28d, 0) as operate_clk_cnt_28d
      |        ,nvl(j.other_clk_cnt_28d, 0) as other_clk_cnt_28d
      |        ,nvl(j.initiative_clk_pct_28d, 0) as initiative_clk_pct_28d
      |        ,nvl(j.alg_clk_pct_28d, 0) as alg_clk_pct_28d
      |        ,nvl(j.operate_clk_pct_28d, 0) as operate_clk_pct_28d
      |        ,nvl(j.other_clk_pct_28d, 0) as other_clk_pct_28d
      |        ,nvl(j.clk_cnt_28d_avg, 0) as clk_cnt_28d_avg
      |        ,nvl(j.initiative_clk_cnt_28d_avg, 0) as initiative_clk_cnt_28d_avg
      |        ,nvl(j.alg_clk_cnt_28d_avg, 0) as alg_clk_cnt_28d_avg
      |        ,nvl(j.operate_clk_cnt_28d_avg, 0) as operate_clk_cnt_28d_avg
      |        ,nvl(j.other_clk_cnt_28d_avg, 0) as other_clk_cnt_28d_avg
      |        ,nvl(j.cnt_ctr_28d, 0) as cnt_ctr_28d
      |        ,nvl(j.initiative_cnt_ctr_28d, 0) as initiative_cnt_ctr_28d
      |        ,nvl(j.alg_cnt_ctr_28d, 0) as alg_cnt_ctr_28d
      |        ,nvl(j.operate_cnt_ctr_28d, 0) as operate_cnt_ctr_28d
      |        ,nvl(j.other_cnt_ctr_28d, 0) as other_cnt_ctr_28d
      |        --歌单歌曲播放来源
      |        ,nvl(d.non_author_initiative_effective_play_cnt_7d_avg, 0) as initiative_non_author_song_effective_play_cnt_7d_avg
      |        ,nvl(d.non_author_alg_effective_play_cnt_7d_avg, 0) as alg_non_author_song_effective_play_cnt_7d_avg
      |        ,nvl(d.non_author_operate_effective_play_cnt_7d_avg, 0) as operate_non_author_song_effective_play_cnt_7d_avg
      |        ,nvl(d.non_author_other_effective_play_cnt_7d_avg, 0) as other_non_author_song_effective_play_cnt_7d_avg
      |        ,nvl(d.non_author_initiative_effective_play_duration_7d_avg, 0) as initiative_non_author_song_effective_play_duration_7d_avg
      |        ,nvl(d.non_author_alg_effective_play_duration_7d_avg, 0) as alg_non_author_song_effective_play_duration_7d_avg
      |        ,nvl(d.non_author_operate_effective_play_duration_7d_avg, 0) as operate_non_author_song_effective_play_duration_7d_avg
      |        ,nvl(d.non_author_other_effective_play_duration_7d_avg, 0) as other_non_author_song_effective_play_duration_7d_avg
      |        ,nvl(d.non_author_initiative_effective_play_song_num_7d, 0) as initiative_non_author_effective_play_song_num_7d
      |        ,nvl(d.non_author_alg_effective_play_song_num_7d, 0) as alg_non_author_effective_play_song_num_7d
      |        ,nvl(d.non_author_operate_effective_play_song_num_7d, 0) as operate_non_author_effective_play_song_num_7d
      |        ,nvl(d.non_author_other_effective_play_song_num_7d, 0) as other_non_author_effective_play_song_num_7d
      |        ,nvl(d.non_author_initiative_effective_play_cnt_7d, 0) as initiative_non_author_song_effective_play_cnt_7d
      |        ,nvl(d.non_author_alg_effective_play_cnt_7d, 0) as alg_non_author_song_effective_play_cnt_7d
      |        ,nvl(d.non_author_operate_effective_play_cnt_7d, 0) as operate_non_author_song_effective_play_cnt_7d
      |        ,nvl(d.non_author_other_effective_play_cnt_7d, 0) as other_non_author_song_effective_play_cnt_7d
      |        ,nvl(d.non_author_initiative_effective_play_duration_7d, 0) as initiative_non_author_song_effective_play_duration_7d
      |        ,nvl(d.non_author_alg_effective_play_duration_7d, 0) as alg_non_author_song_effective_play_duration_7d
      |        ,nvl(d.non_author_operate_effective_play_duration_7d, 0) as operate_non_author_song_effective_play_duration_7d
      |        ,nvl(d.non_author_other_effective_play_duration_7d, 0) as other_non_author_song_effective_play_duration_7d
      |        ,if(nvl(j.exp_cnt_7d, 0) = 0, 0, nvl(d.non_author_effective_play_cnt_7d, 0) / nvl(j.exp_cnt_7d, 0)) as non_author_song_effective_play_ratio_7d
      |        ,if(nvl(j.initiative_exp_cnt_7d, 0) = 0, 0, nvl(d.non_author_initiative_effective_play_cnt_7d, 0) / nvl(j.initiative_exp_cnt_7d, 0)) as initiative_non_author_song_effective_play_ratio_7d
      |        ,if(nvl(j.alg_exp_cnt_7d, 0) = 0, 0, nvl(d.non_author_alg_effective_play_cnt_7d, 0) / nvl(j.alg_exp_cnt_7d, 0)) as alg_non_author_song_effective_play_ratio_7d
      |        ,if(nvl(j.operate_exp_cnt_7d, 0) = 0, 0, nvl(d.non_author_operate_effective_play_cnt_7d, 0) / nvl(j.operate_exp_cnt_7d, 0)) as operate_non_author_song_effective_play_ratio_7d
      |        ,if(nvl(j.other_exp_cnt_7d, 0) = 0, 0, nvl(d.non_author_other_effective_play_cnt_7d, 0) / nvl(j.other_exp_cnt_7d, 0)) as other_non_author_song_effective_play_ratio_7d
      |        ,nvl(d.non_author_initiative_effective_play_cnt_28d_avg, 0) as initiative_non_author_song_effective_play_cnt_28d_avg
      |        ,nvl(d.non_author_alg_effective_play_cnt_28d_avg, 0) as alg_non_author_song_effective_play_cnt_28d_avg
      |        ,nvl(d.non_author_operate_effective_play_cnt_28d_avg, 0) as operate_non_author_song_effective_play_cnt_28d_avg
      |        ,nvl(d.non_author_other_effective_play_cnt_28d_avg, 0) as other_non_author_song_effective_play_cnt_28d_avg
      |        ,nvl(d.non_author_initiative_effective_play_duration_28d_avg, 0) as initiative_non_author_song_effective_play_duration_28d_avg
      |        ,nvl(d.non_author_alg_effective_play_duration_28d_avg, 0) as alg_non_author_song_effective_play_duration_28d_avg
      |        ,nvl(d.non_author_operate_effective_play_duration_28d_avg, 0) as operate_non_author_song_effective_play_duration_28d_avg
      |        ,nvl(d.non_author_other_effective_play_duration_28d_avg, 0) as other_non_author_song_effective_play_duration_28d_avg
      |        ,nvl(d.non_author_initiative_effective_play_song_num_28d, 0) as initiative_non_author_effective_play_song_num_28d
      |        ,nvl(d.non_author_alg_effective_play_song_num_28d, 0) as alg_non_author_effective_play_song_num_28d
      |        ,nvl(d.non_author_operate_effective_play_song_num_28d, 0) as operate_non_author_effective_play_song_num_28d
      |        ,nvl(d.non_author_other_effective_play_song_num_28d, 0) as other_non_author_effective_play_song_num_28d
      |        ,nvl(d.non_author_initiative_effective_play_cnt_28d, 0) as initiative_non_author_song_effective_play_cnt_28d
      |        ,nvl(d.non_author_alg_effective_play_cnt_28d, 0) as alg_non_author_song_effective_play_cnt_28d
      |        ,nvl(d.non_author_operate_effective_play_cnt_28d, 0) as operate_non_author_song_effective_play_cnt_28d
      |        ,nvl(d.non_author_other_effective_play_cnt_28d, 0) as other_non_author_song_effective_play_cnt_28d
      |        ,nvl(d.non_author_initiative_effective_play_duration_28d, 0) as initiative_non_author_song_effective_play_duration_28d
      |        ,nvl(d.non_author_alg_effective_play_duration_28d, 0) as alg_non_author_song_effective_play_duration_28d
      |        ,nvl(d.non_author_operate_effective_play_duration_28d, 0) as operate_non_author_song_effective_play_duration_28d
      |        ,nvl(d.non_author_other_effective_play_duration_28d, 0) as other_non_author_song_effective_play_duration_28d
      |        ,if(nvl(j.exp_cnt_28d, 0) = 0, 0, nvl(d.non_author_effective_play_cnt_28d, 0) / nvl(j.exp_cnt_28d, 0)) as non_author_song_effective_play_ratio_28d
      |        ,if(nvl(j.initiative_exp_cnt_28d, 0) = 0, 0, nvl(d.non_author_initiative_effective_play_cnt_28d, 0) / nvl(j.initiative_exp_cnt_28d, 0)) as initiative_non_author_song_effective_play_ratio_28d
      |        ,if(nvl(j.alg_exp_cnt_28d, 0) = 0, 0, nvl(d.non_author_alg_effective_play_cnt_28d, 0) / nvl(j.alg_exp_cnt_28d, 0)) as alg_non_author_song_effective_play_ratio_28d
      |        ,if(nvl(j.operate_exp_cnt_28d, 0) = 0, 0, nvl(d.non_author_operate_effective_play_cnt_28d, 0) / nvl(j.operate_exp_cnt_28d, 0)) as operate_non_author_song_effective_play_ratio_28d
      |        ,if(nvl(j.other_exp_cnt_28d, 0) = 0, 0, nvl(d.non_author_other_effective_play_cnt_28d, 0) / nvl(j.other_exp_cnt_28d, 0)) as other_non_author_song_effective_play_ratio_28d
      |        ,nvl(c.valid_song_num, 0) / nvl(a.online_song_num, 0) as valid_song_rate --灰色版权歌曲比例
      |        --集中度标签
      |        ,null as list_top25_song_effective_play_pct_level_28d  --作废
      |        ,null as list_top25_user_effective_play_pct_level_28d  --作废
      |        --新客首播次数
      |        ,nvl(k.first_song_play_new_unt_1d, 0) as first_song_play_unt_1d --待调整！！！调整会整体首播
      |        ,nvl(k.first_song_play_new_unt_1d, 0) as first_song_play_new_unt_1d
      |        ,nvl(k.first_song_play_recall_unt_1d, 0) as first_song_play_recall_unt_1d --待调整！！！
      |        ,nvl(k.first_song_play_retain_unt_1d, 0) as first_song_play_retain_unt_1d --待调整！！！
      |        --歌单中歌曲播放，v2.0
      |        ,nvl(d.non_author_initiative_effective_play_cnt_1d, 0) as non_author_initiative_effective_play_cnt_1d
      |        ,nvl(d.non_author_initiative_effective_play_duration_1d, 0) as non_author_initiative_effective_play_duration_1d
      |        ,nvl(d.non_author_alg_effective_play_cnt_1d, 0) as non_author_alg_effective_play_cnt_1d
      |        ,nvl(d.non_author_alg_effective_play_duration_1d, 0) as non_author_alg_effective_play_duration_1d
      |        ,nvl(d.non_author_operate_effective_play_cnt_1d, 0) as non_author_operate_effective_play_cnt_1d
      |        ,nvl(d.non_author_operate_effective_play_duration_1d, 0) as non_author_operate_effective_play_duration_1d
      |        ,nvl(d.non_author_other_effective_play_cnt_1d, 0) as non_author_other_effective_play_cnt_1d
      |        ,nvl(d.non_author_other_effective_play_duration_1d, 0) as non_author_other_effective_play_duration_1d
      |        ,nvl(d.non_author_initiative_effective_play_song_num_1d, 0) as non_author_initiative_effective_play_song_num_1d
      |        ,nvl(d.non_author_alg_effective_play_song_num_1d, 0) as non_author_alg_effective_play_song_num_1d
      |        ,nvl(d.non_author_operate_effective_play_song_num_1d, 0) as non_author_operate_effective_play_song_num_1d
      |        ,nvl(d.non_author_other_effective_play_song_num_1d, 0) as non_author_other_effective_play_song_num_1d
      |        ,nvl(d.non_author_play_cnt_1d, 0) as non_author_song_play_cnt_1d
      |        ,nvl(d.non_author_effective_play_cnt_1d, 0) as non_author_song_effective_play_cnt_1d
      |        ,nvl(d.non_author_effective_play_duration_1d, 0) as non_author_song_effective_play_duration_1d
      |        ,nvl(d.non_author_full_play_cnt_1d, 0) as non_author_song_full_play_cnt_1d
      |        ,nvl(d.non_author_full_play_duration_1d, 0) as non_author_song_full_play_duration_1d
      |        ,nvl(d.non_author_like_cnt_1d, 0) as non_author_song_like_cnt_1d
      |        ,nvl(d.non_author_effective_play_song_num_1d, 0) as non_author_effective_play_song_num_1d
      |        ,nvl(d.non_author_full_play_song_num_1d, 0) as non_author_full_play_song_num_1d
      |        ,nvl(d.non_author_like_song_num_1d, 0) as non_author_like_song_num_1d
      |        ,nvl(d.non_author_play_cnt_7d, 0) as non_author_song_play_cnt_7d
      |        ,nvl(d.non_author_full_play_song_num_7d, 0) as non_author_full_play_song_num_7d
      |        ,nvl(d.non_author_full_play_cnt_7d, 0) as non_author_song_full_play_cnt_7d
      |        ,nvl(d.non_author_full_play_duration_7d, 0) as non_author_song_full_play_duration_7d
      |        ,nvl(d.non_author_like_song_num_7d, 0) as non_author_like_song_num_7d
      |        ,nvl(d.non_author_like_cnt_7d, 0) as non_author_song_like_cnt_7d
      |        ,nvl(d.non_author_play_cnt_28d, 0) as non_author_song_play_cnt_28d
      |        ,nvl(d.non_author_full_play_song_num_28d, 0) as non_author_full_play_song_num_28d
      |        ,nvl(d.non_author_full_play_cnt_28d, 0) as non_author_song_full_play_cnt_28d
      |        ,nvl(d.non_author_full_play_duration_28d, 0) as non_author_song_full_play_duration_28d
      |        ,nvl(d.non_author_like_song_num_28d, 0) as non_author_like_song_num_28d
      |        ,nvl(d.non_author_like_cnt_28d, 0) as non_author_song_like_cnt_28d
      |        --歌单中歌曲播放人数
      |        ,nvl(l.non_author_song_play_unt_1d, 0) as non_author_song_play_unt_1d
      |        ,nvl(l.non_author_song_effective_play_unt_1d, 0) as non_author_song_effective_play_unt_1d
      |        ,nvl(l.non_author_song_full_play_unt_1d, 0) as non_author_song_full_play_unt_1d
      |        ,nvl(l.non_author_song_like_unt_1d, 0) as non_author_song_like_unt_1d
      |        ,nvl(l.non_author_song_play_unt_7d, 0) as non_author_song_play_unt_7d
      |        ,nvl(l.non_author_song_effective_play_unt_7d, 0) as non_author_song_effective_play_unt_7d
      |        ,nvl(l.non_author_song_full_play_unt_7d, 0) as non_author_song_full_play_unt_7d
      |        ,nvl(l.non_author_song_like_unt_7d, 0) as non_author_song_like_unt_7d
      |        ,nvl(l.non_author_song_play_unt_28d, 0) as non_author_song_play_unt_28d
      |        ,nvl(l.non_author_song_effective_play_unt_28d, 0) as non_author_song_effective_play_unt_28d
      |        ,nvl(l.non_author_song_full_play_unt_28d, 0) as non_author_song_full_play_unt_28d
      |        ,nvl(l.non_author_song_like_unt_28d, 0) as non_author_song_like_unt_28d
      |        ,if(nvl(l.non_author_song_effective_play_unt_1d, 0) = 0, 0, nvl(l.non_author_song_like_unt_1d, 0) / nvl(l.non_author_song_effective_play_unt_1d, 0)) as non_author_like_unt_rate_1d
      |        ,if(nvl(l.non_author_song_effective_play_unt_7d, 0) = 0, 0, nvl(l.non_author_song_like_unt_7d, 0) / nvl(l.non_author_song_effective_play_unt_7d, 0)) as non_author_like_unt_rate_7d
      |        ,if(nvl(l.non_author_song_effective_play_unt_28d, 0) = 0, 0, nvl(l.non_author_song_like_unt_28d, 0) / nvl(l.non_author_song_effective_play_unt_28d, 0)) as non_author_like_unt_rate_28d
      |        --歌单中歌曲播放频次，v2.0
      |        ,if(nvl(l.non_author_song_effective_play_unt_1d, 0) = 0, 0, nvl(d.non_author_effective_play_cnt_1d, 0) / nvl(l.non_author_song_effective_play_unt_1d, 0)) as non_author_song_effective_play_cnt_1d_user_avg
      |        ,if(nvl(l.non_author_song_full_play_unt_1d, 0) = 0, 0, nvl(d.non_author_full_play_cnt_1d, 0) / nvl(l.non_author_song_full_play_unt_1d, 0)) as non_author_song_full_play_cnt_1d_user_avg
      |        ,if(nvl(l.non_author_song_effective_play_unt_1d, 0) = 0, 0, nvl(d.non_author_effective_play_duration_1d, 0) / nvl(l.non_author_song_effective_play_unt_1d, 0)) as non_author_song_effective_play_duration_1d_user_avg
      |        ,if(nvl(l.non_author_song_full_play_unt_1d, 0) = 0, 0, nvl(d.non_author_full_play_duration_1d, 0) / nvl(l.non_author_song_full_play_unt_1d, 0)) as non_author_song_full_play_duration_1d_user_avg
      |        ,nvl(l.non_author_effective_play_song_num_1d_user_avg, 0) as non_author_effective_play_song_num_1d_user_avg
      |        ,nvl(l.non_author_full_play_song_num_1d_user_avg, 0) as non_author_full_play_song_num_1d_user_avg
      |        ,if(nvl(d.non_author_effective_play_song_num_1d, 0) = 0, 0, nvl(d.non_author_effective_play_cnt_1d, 0) / nvl(d.non_author_effective_play_song_num_1d, 0)) as non_author_song_effective_play_cnt_1d_song_avg
      |        ,if(nvl(d.non_author_full_play_song_num_1d, 0) = 0, 0, nvl(d.non_author_full_play_cnt_1d, 0) / nvl(d.non_author_full_play_song_num_1d, 0)) as non_author_song_full_play_cnt_1d_song_avg
      |        ,if(nvl(d.non_author_effective_play_song_num_1d, 0) = 0, 0, nvl(d.non_author_effective_play_duration_1d, 0) / nvl(d.non_author_effective_play_song_num_1d, 0)) as non_author_song_effective_play_duration_1d_song_avg
      |        ,if(nvl(d.non_author_full_play_song_num_1d, 0) = 0, 0, nvl(d.non_author_full_play_duration_1d, 0) / nvl(d.non_author_full_play_song_num_1d, 0)) as non_author_song_full_play_duration_1d_song_avg
      |        ,nvl(d.non_author_song_effective_play_unt_1d_song_avg, 0) as non_author_song_effective_play_unt_1d_song_avg
      |        ,nvl(d.non_author_song_full_play_unt_1d_song_avg, 0) as non_author_song_full_play_unt_1d_song_avg
      |        --歌单互动，v2.0
      |        ,if(nvl(l.non_author_song_effective_play_unt_1d, 0) = 0, 0, nvl(a.collect_unt_1d, 0) / nvl(l.non_author_song_effective_play_unt_1d, 0)) as collect_unt_rate_1d
      |        ,if(nvl(l.non_author_song_effective_play_unt_7d, 0) = 0, 0, nvl(a.collect_unt_7d, 0) / nvl(l.non_author_song_effective_play_unt_7d, 0)) as collect_unt_rate_7d
      |        ,if(nvl(l.non_author_song_effective_play_unt_28d, 0) = 0, 0, nvl(a.collect_unt_28d, 0) / nvl(l.non_author_song_effective_play_unt_28d, 0)) as collect_unt_rate_28d
      |        --歌曲统计
      |        ,a.ts_song_num --需要成为会员才能听
      |        ,a.triable_ts_song_num
      |        ,a.copyrighted_song_num
      |        ,nvl(b.create_song_num_90d, 0) as create_song_num_90d
      |        ,nvl(b.create_song_days_90d, 0) as create_song_days_90d
      |        ,case   when b.last_create_date is not null then b.last_create_date
      |                when a.playlist_type = '100' then '每日算法更新'
      |                else null
      |                end as last_create_date
      |        ,nvl(m.main_language_genre_tag_num, 0) as main_language_genre_tag_num
      |        ,m.main_language_genre_tag_list as main_language_genre_tag_list
      |from    (
      |        select  *
      |        from    tmp_ads_itm_muse_playlist_tag_dd
      |        where   dt = '2022-01-01'
      |        ) a
      |""".stripMargin

  // noinspection ScalaStyle
  val sql2 =
    """
      |left join (--歌单x歌曲表，该底表不包含官方歌单的数据，1天1100g
      |        select  dt
      |                ,playlist_id
      |                ,sum(case when to_date(create_time) = '2022-01-01' then 1
      |                                else 0
      |                                end) as create_song_num_1d
      |                ,sum(case when to_date(create_time) between date_sub('2022-01-01', 6) and '2022-01-01' then 1
      |                                else 0
      |                                end) as create_song_num_7d
      |                ,sum(case when to_date(create_time) between date_sub('2022-01-01', 27) and '2022-01-01' then 1
      |                                else 0
      |                                end) as create_song_num_28d
      |                ,count(distinct case when to_date(create_time) between date_sub('2022-01-01', 27) and '2022-01-01' then to_date(create_time)
      |                                else null
      |                                end) as create_song_days_28d
      |                ,sum(case when to_date(create_time) between date_sub('2022-01-01', 59) and '2022-01-01' then 1
      |                                else 0
      |                                end) as create_song_num_60d
      |                ,count(distinct case when to_date(create_time) between date_sub('2022-01-01', 59) and '2022-01-01' then to_date(create_time)
      |                                else null
      |                                end) as create_song_days_60d
      |                ,sum(case when to_date(create_time) between date_sub('2022-01-01', 89) and '2022-01-01' then 1
      |                                else 0
      |                                end) as create_song_num_90d
      |                ,count(distinct case when to_date(create_time) between date_sub('2022-01-01', 89) and '2022-01-01' then to_date(create_time)
      |                                else null
      |                                end) as create_song_days_90d
      |                ,sum(case when to_date(create_time) between date_sub('2022-01-01', 179) and '2022-01-01' then 1
      |                                else 0
      |                                end) as create_song_num_180d
      |                ,count(1) as collect_song_num --歌曲数
      |                ,count(distinct to_date(create_time)) as create_song_days
      |                ,max(to_date(create_time)) as last_create_date
      |        from    tmp_ads_itm_muse_playlist_song_list_dd
      |        where   dt = '2022-01-01'
      |        group by dt
      |                ,playlist_id
      |        ) b
      |on      a.dt = b.dt
      |and     a.playlist_id = b.playlist_id
      |left join (--muse歌曲标签表，1天30g，歌单x歌曲表只看有播放的歌曲数据？？？考虑一下……
      |        select  a.dt
      |                ,a.playlist_id
      |                ,count(1) as total_song_num
      |                ,sum(if(nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 900000, 1, 0)) as play_rank_a_plus_song_num
      |                ,sum(if(nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 100000 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 900000, 1, 0)) as play_rank_middle_song_num
      |                ,sum(if(nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 5000 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 100000, 1, 0)) as play_rank_small_song_num
      |                ,sum(if(nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 5000, 1, 0)) as play_rank_ssmall_song_num
      |                ,sum(if(b.play_rank = 'SS', 1, 0)) as play_rank_ss_song_num
      |                ,sum(if(b.play_rank = 'S', 1, 0)) as play_rank_s_song_num
      |                ,sum(if(b.play_rank = 'A', 1, 0)) as play_rank_a_song_num
      |                ,sum(if(b.play_rank = 'B', 1, 0)) as play_rank_b_song_num
      |                ,sum(if(b.play_rank = 'C', 1, 0)) as play_rank_c_song_num
      |                ,sum(if(b.play_rank = 'D', 1, 0)) as play_rank_d_song_num
      |                ,sum(if(b.play_rank = 'E', 1, 0)) as play_rank_e_song_num
      |                ,sum(if(b.play_rank = 'F', 1, 0)) as play_rank_f_song_num
      |                ,sum(if(b.play_rank = 'G', 1, 0)) as play_rank_g_song_num
      |                ,sum(if(b.play_rank = 'H', 1, 0)) as play_rank_h_song_num
      |                ,sum(if(b.online_status not in ('-1', '0', '7'), 1, 0)) as valid_song_num
      |                ,sum(if(b.record_auth_status = '-1', 1, 0)) as gray_auth_song_num
      |        from    (
      |                select  dt
      |                        ,playlist_id
      |                        ,song_id
      |                from    tmp_ads_itm_muse_playlist_song_list_dd
      |                where   dt = '2022-01-01'
      |                ) a
      |        left join (
      |                select  dt
      |                        ,song_id
      |                        ,play_rank
      |                        ,online_status
      |                        ,record_auth_status  --录音版权状态，2-独家，1-非独家，0-不在有效期内（不包含续约中），-1-灰色版权
      |                        ,effective_play_cnt_1d_avg_l7d  --7日均有效播放次数
      |                from    ads_itm_pgc_song_muse_tag_dd
      |                where   dt = '2022-01-01'
      |                ) b
      |        on      a.dt = b.dt
      |        and     a.song_id = b.song_id
      |        group by a.dt
      |                ,a.playlist_id
      |        ) c
      |on      a.dt = c.dt
      |and     a.playlist_id = c.playlist_id
      |left join (
      |        select  *
      |                --7d
      |                ,non_author_play_cnt_7d / 7 as non_author_play_cnt_7d_avg
      |                ,non_author_effective_play_cnt_7d / 7 as non_author_effective_play_cnt_7d_avg
      |                ,non_author_effective_play_duration_7d / 7 as non_author_effective_play_duration_7d_avg
      |                ,non_author_full_play_cnt_7d / 7 as non_author_full_play_cnt_7d_avg
      |                ,non_author_full_play_duration_7d / 7 as non_author_full_play_duration_7d_avg
      |                ,non_author_like_cnt_7d / 7 as non_author_like_cnt_7d_avg
      |                --28d
      |                ,non_author_play_cnt_28d / 28 as non_author_play_cnt_28d_avg
      |                ,non_author_effective_play_cnt_28d / 28 as non_author_effective_play_cnt_28d_avg
      |                ,non_author_effective_play_duration_28d / 28 as non_author_effective_play_duration_28d_avg
      |                ,non_author_full_play_cnt_28d / 28 as non_author_full_play_cnt_28d_avg
      |                ,non_author_full_play_duration_28d / 28 as non_author_full_play_duration_28d_avg
      |                ,non_author_like_cnt_28d / 28 as non_author_like_cnt_28d_avg
      |                ,non_author_a_plus_effective_play_song_num_28d / non_author_effective_play_song_num_28d as non_author_a_plus_effective_play_song_pct_28d
      |                ,non_author_middle_effective_play_song_num_28d / non_author_effective_play_song_num_28d as non_author_middle_effective_play_song_pct_28d
      |                ,non_author_small_effective_play_song_num_28d / non_author_effective_play_song_num_28d as non_author_small_effective_play_song_pct_28d
      |                ,non_author_ssmall_effective_play_song_num_28d / non_author_effective_play_song_num_28d as non_author_ssmall_effective_play_song_pct_28d
      |                --资源位
      |                ,non_author_initiative_effective_play_cnt_7d / 7 as non_author_initiative_effective_play_cnt_7d_avg
      |                ,non_author_alg_effective_play_cnt_7d / 7 as non_author_alg_effective_play_cnt_7d_avg
      |                ,non_author_operate_effective_play_cnt_7d / 7 as non_author_operate_effective_play_cnt_7d_avg
      |                ,non_author_other_effective_play_cnt_7d / 7 as non_author_other_effective_play_cnt_7d_avg
      |                ,non_author_initiative_effective_play_duration_7d / 7 as non_author_initiative_effective_play_duration_7d_avg
      |                ,non_author_alg_effective_play_duration_7d / 7 as non_author_alg_effective_play_duration_7d_avg
      |                ,non_author_operate_effective_play_duration_7d / 7 as non_author_operate_effective_play_duration_7d_avg
      |                ,non_author_other_effective_play_duration_7d / 7 as non_author_other_effective_play_duration_7d_avg
      |                ,non_author_initiative_effective_play_cnt_28d / 28 as non_author_initiative_effective_play_cnt_28d_avg
      |                ,non_author_alg_effective_play_cnt_28d / 28 as non_author_alg_effective_play_cnt_28d_avg
      |                ,non_author_operate_effective_play_cnt_28d / 28 as non_author_operate_effective_play_cnt_28d_avg
      |                ,non_author_other_effective_play_cnt_28d / 28 as non_author_other_effective_play_cnt_28d_avg
      |                ,non_author_initiative_effective_play_duration_28d / 28 as non_author_initiative_effective_play_duration_28d_avg
      |                ,non_author_alg_effective_play_duration_28d / 28 as non_author_alg_effective_play_duration_28d_avg
      |                ,non_author_operate_effective_play_duration_28d / 28 as non_author_operate_effective_play_duration_28d_avg
      |                ,non_author_other_effective_play_duration_28d / 28 as non_author_other_effective_play_duration_28d_avg
      |        from    (
      |                select  a.dt
      |                        ,a.playlist_id
      |                        --1d，整体
      |                        ,sum(a.non_author_play_cnt_1d) as non_author_play_cnt_1d
      |                        ,sum(a.non_author_effective_play_cnt_1d) as non_author_effective_play_cnt_1d
      |                        ,sum(a.non_author_effective_play_duration_1d) as non_author_effective_play_duration_1d
      |                        ,sum(a.non_author_full_play_cnt_1d) as non_author_full_play_cnt_1d
      |                        ,sum(a.non_author_full_play_duration_1d) as non_author_full_play_duration_1d
      |                        ,sum(a.non_author_like_cnt_1d) as non_author_like_cnt_1d
      |                        ,sum(if(a.non_author_effective_play_cnt_1d > 0, 1, 0)) as non_author_effective_play_song_num_1d
      |                        ,sum(if(a.non_author_full_play_cnt_1d > 0, 1, 0)) as non_author_full_play_song_num_1d
      |                        ,sum(if(a.non_author_like_cnt_1d > 0, 1, 0)) as non_author_like_song_num_1d
      |                        --1d，分资源
      |                        ,sum(a.non_author_initiative_effective_play_cnt_1d) as non_author_initiative_effective_play_cnt_1d
      |                        ,sum(a.non_author_initiative_effective_play_duration_1d) as non_author_initiative_effective_play_duration_1d
      |                        ,sum(if(a.non_author_initiative_effective_play_cnt_1d > 0, 1, 0)) as non_author_initiative_effective_play_song_num_1d
      |                        ,sum(a.non_author_alg_effective_play_cnt_1d) as non_author_alg_effective_play_cnt_1d
      |                        ,sum(a.non_author_alg_effective_play_duration_1d) as non_author_alg_effective_play_duration_1d
      |                        ,sum(if(a.non_author_alg_effective_play_cnt_1d > 0, 1, 0)) as non_author_alg_effective_play_song_num_1d
      |                        ,sum(a.non_author_operate_effective_play_cnt_1d) as non_author_operate_effective_play_cnt_1d
      |                        ,sum(a.non_author_operate_effective_play_duration_1d) as non_author_operate_effective_play_duration_1d
      |                        ,sum(if(a.non_author_operate_effective_play_cnt_1d > 0, 1, 0)) as non_author_operate_effective_play_song_num_1d
      |                        ,sum(a.non_author_other_effective_play_cnt_1d) as non_author_other_effective_play_cnt_1d
      |                        ,sum(a.non_author_other_effective_play_duration_1d) as non_author_other_effective_play_duration_1d
      |                        ,sum(if(a.non_author_other_effective_play_cnt_1d > 0, 1, 0)) as non_author_other_effective_play_song_num_1d
      |                        --7d，整体
      |                        ,sum(a.non_author_play_cnt_7d) as non_author_play_cnt_7d
      |                        ,sum(a.non_author_effective_play_cnt_7d) as non_author_effective_play_cnt_7d
      |                        ,sum(a.non_author_effective_play_duration_7d) as non_author_effective_play_duration_7d
      |                        ,sum(a.non_author_full_play_cnt_7d) as non_author_full_play_cnt_7d
      |                        ,sum(a.non_author_full_play_duration_7d) as non_author_full_play_duration_7d
      |                        ,sum(a.non_author_like_cnt_7d) as non_author_like_cnt_7d
      |                        ,sum(if(a.non_author_effective_play_cnt_7d > 0, 1, 0)) as non_author_effective_play_song_num_7d
      |                        ,sum(if(a.non_author_full_play_cnt_7d > 0, 1, 0)) as non_author_full_play_song_num_7d
      |                        ,sum(if(a.non_author_like_cnt_7d > 0, 1, 0)) as non_author_like_song_num_7d
      |                        --7d，分资源
      |                        ,sum(a.non_author_initiative_effective_play_cnt_7d) as non_author_initiative_effective_play_cnt_7d
      |                        ,sum(a.non_author_initiative_effective_play_duration_7d) as non_author_initiative_effective_play_duration_7d
      |                        ,sum(if(a.non_author_initiative_effective_play_cnt_7d > 0, 1, 0)) as non_author_initiative_effective_play_song_num_7d
      |                        ,sum(a.non_author_alg_effective_play_cnt_7d) as non_author_alg_effective_play_cnt_7d
      |                        ,sum(a.non_author_alg_effective_play_duration_7d) as non_author_alg_effective_play_duration_7d
      |                        ,sum(if(a.non_author_alg_effective_play_cnt_7d > 0, 1, 0)) as non_author_alg_effective_play_song_num_7d
      |                        ,sum(a.non_author_operate_effective_play_cnt_7d) as non_author_operate_effective_play_cnt_7d
      |                        ,sum(a.non_author_operate_effective_play_duration_7d) as non_author_operate_effective_play_duration_7d
      |                        ,sum(if(a.non_author_operate_effective_play_cnt_7d > 0, 1, 0)) as non_author_operate_effective_play_song_num_7d
      |                        ,sum(a.non_author_other_effective_play_cnt_7d) as non_author_other_effective_play_cnt_7d
      |                        ,sum(a.non_author_other_effective_play_duration_7d) as non_author_other_effective_play_duration_7d
      |                        ,sum(if(a.non_author_other_effective_play_cnt_7d > 0, 1, 0)) as non_author_other_effective_play_song_num_7d
      |                        --28d，整体
      |                        ,sum(a.non_author_play_cnt_28d) as non_author_play_cnt_28d
      |                        ,sum(a.non_author_effective_play_cnt_28d) as non_author_effective_play_cnt_28d
      |                        ,sum(a.non_author_effective_play_duration_28d) as non_author_effective_play_duration_28d
      |                        ,sum(a.non_author_full_play_cnt_28d) as non_author_full_play_cnt_28d
      |                        ,sum(a.non_author_full_play_duration_28d) as non_author_full_play_duration_28d
      |                        ,sum(a.non_author_like_cnt_28d) as non_author_like_cnt_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0, 1, 0)) as non_author_effective_play_song_num_28d
      |                        ,sum(if(a.non_author_full_play_cnt_28d > 0, 1, 0)) as non_author_full_play_song_num_28d
      |                        ,sum(if(a.non_author_like_cnt_28d > 0, 1, 0)) as non_author_like_song_num_28d
      |                        --28d，分资源
      |                        ,sum(a.non_author_initiative_effective_play_cnt_28d) as non_author_initiative_effective_play_cnt_28d
      |                        ,sum(a.non_author_initiative_effective_play_duration_28d) as non_author_initiative_effective_play_duration_28d
      |                        ,sum(if(a.non_author_initiative_effective_play_cnt_28d > 0, 1, 0)) as non_author_initiative_effective_play_song_num_28d
      |                        ,sum(a.non_author_alg_effective_play_cnt_28d) as non_author_alg_effective_play_cnt_28d
      |                        ,sum(a.non_author_alg_effective_play_duration_28d) as non_author_alg_effective_play_duration_28d
      |                        ,sum(if(a.non_author_alg_effective_play_cnt_28d > 0, 1, 0)) as non_author_alg_effective_play_song_num_28d
      |                        ,sum(a.non_author_operate_effective_play_cnt_28d) as non_author_operate_effective_play_cnt_28d
      |                        ,sum(a.non_author_operate_effective_play_duration_28d) as non_author_operate_effective_play_duration_28d
      |                        ,sum(if(a.non_author_operate_effective_play_cnt_28d > 0, 1, 0)) as non_author_operate_effective_play_song_num_28d
      |                        ,sum(a.non_author_other_effective_play_cnt_28d) as non_author_other_effective_play_cnt_28d
      |                        ,sum(a.non_author_other_effective_play_duration_28d) as non_author_other_effective_play_duration_28d
      |                        ,sum(if(a.non_author_other_effective_play_cnt_28d > 0, 1, 0)) as non_author_other_effective_play_song_num_28d
      |                        --28d，等级分布
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 900000, 1, 0)) as non_author_a_plus_effective_play_song_num_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 100000 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 900000, 1, 0)) as non_author_middle_effective_play_song_num_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 5000 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 100000, 1, 0)) as non_author_small_effective_play_song_num_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 5000, 1, 0)) as non_author_ssmall_effective_play_song_num_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 900000, a.non_author_effective_play_cnt_28d, 0)) as non_author_a_plus_effective_play_cnt_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 100000 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 900000, a.non_author_effective_play_cnt_28d, 0)) as non_author_middle_effective_play_cnt_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 5000 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 100000, a.non_author_effective_play_cnt_28d, 0)) as non_author_small_effective_play_cnt_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 5000, a.non_author_effective_play_cnt_28d, 0)) as non_author_ssmall_effective_play_cnt_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 900000, a.non_author_effective_play_duration_28d, 0)) as non_author_a_plus_effective_play_duration_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 100000 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 900000, a.non_author_effective_play_duration_28d, 0)) as non_author_middle_effective_play_duration_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) >= 5000 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 100000, a.non_author_effective_play_duration_28d, 0)) as non_author_small_effective_play_duration_28d
      |                        ,sum(if(a.non_author_effective_play_cnt_28d > 0 and nvl(b.effective_play_cnt_1d_avg_l7d, 0) < 5000, a.non_author_effective_play_duration_28d, 0)) as non_author_ssmall_effective_play_duration_28d
      |                        --转化率
      |                        ,if(sum(play_cnt_1d) = 0, 0, sum(effective_play_cnt_1d) / sum(play_cnt_1d)) as effective_play_cnt_1d_rate_avg
      |                        ,if(sum(play_cnt_7d) = 0, 0, sum(effective_play_cnt_7d) / sum(play_cnt_7d)) as effective_play_cnt_7d_rate_avg
      |                        ,if(sum(play_cnt_28d) = 0, 0, sum(effective_play_cnt_28d) / sum(play_cnt_28d)) as effective_play_cnt_28d_rate_avg
      |                        ,if(sum(play_cnt_1d) = 0, 0, sum(full_play_cnt_1d) / sum(play_cnt_1d)) as full_play_cnt_1d_rate_avg
      |                        ,if(sum(play_cnt_7d) = 0, 0, sum(full_play_cnt_7d) / sum(play_cnt_7d)) as full_play_cnt_7d_rate_avg
      |                        ,if(sum(play_cnt_28d) = 0, 0, sum(full_play_cnt_28d) / sum(play_cnt_28d)) as full_play_cnt_28d_rate_avg
      |                        ,if(sum(effective_play_cnt_1d) = 0, 0, sum(like_cnt_1d) / sum(effective_play_cnt_1d)) as like_cnt_1d_rate_avg
      |                        ,if(sum(effective_play_cnt_7d) = 0, 0, sum(like_cnt_7d) / sum(effective_play_cnt_7d)) as like_cnt_7d_rate_avg
      |                        ,if(sum(effective_play_cnt_28d) = 0, 0, sum(like_cnt_28d) / sum(effective_play_cnt_28d)) as like_cnt_28d_rate_avg
      |                        ,if(sum(non_author_play_cnt_1d) = 0, 0, sum(non_author_effective_play_cnt_1d) / sum(non_author_play_cnt_1d)) as non_author_effective_play_cnt_1d_rate_avg
      |                        ,if(sum(non_author_play_cnt_7d) = 0, 0, sum(non_author_effective_play_cnt_7d) / sum(non_author_play_cnt_7d)) as non_author_effective_play_cnt_7d_rate_avg
      |                        ,if(sum(non_author_play_cnt_28d) = 0, 0, sum(non_author_effective_play_cnt_28d) / sum(non_author_play_cnt_28d)) as non_author_effective_play_cnt_28d_rate_avg
      |                        ,if(sum(non_author_play_cnt_1d) = 0, 0, sum(non_author_full_play_cnt_1d) / sum(non_author_play_cnt_1d)) as non_author_full_play_cnt_1d_rate_avg
      |                        ,if(sum(non_author_play_cnt_7d) = 0, 0, sum(non_author_full_play_cnt_7d) / sum(non_author_play_cnt_7d)) as non_author_full_play_cnt_7d_rate_avg
      |                        ,if(sum(non_author_play_cnt_28d) = 0, 0, sum(non_author_full_play_cnt_28d) / sum(non_author_play_cnt_28d)) as non_author_full_play_cnt_28d_rate_avg
      |                        ,if(sum(non_author_effective_play_cnt_1d) = 0, 0, sum(non_author_like_cnt_1d ) / sum(non_author_effective_play_cnt_1d)) as non_author_like_cnt_1d_rate_avg
      |                        ,if(sum(non_author_effective_play_cnt_7d) = 0, 0, sum(non_author_like_cnt_7d ) / sum(non_author_effective_play_cnt_7d)) as non_author_like_cnt_7d_rate_avg
      |                        ,if(sum(non_author_effective_play_cnt_28d) = 0, 0, sum(non_author_like_cnt_28d ) / sum(non_author_effective_play_cnt_28d)) as non_author_like_cnt_28d_rate_avg
      |                        --歌曲人均播放次数
      |                        ,if(sum(a.non_author_effective_play_cnt_1d) = 0, 0, sum(a.non_author_effective_play_unt_1d) / sum(if(a.non_author_effective_play_cnt_1d > 0, 1, 0))) as non_author_song_effective_play_unt_1d_song_avg
      |                        ,if(sum(a.non_author_full_play_cnt_1d) = 0, 0, sum(a.non_author_full_play_unt_1d) / sum(if(a.non_author_full_play_cnt_1d > 0, 1, 0))) as non_author_song_full_play_unt_1d_song_avg
      |                from    (
      |                        select  *
      |                        from    tmp_ads_itm_muse_playlist_song_tag_summary
      |                        where   dt = '2022-01-01'
      |                        ) a
      |                left join (
      |                        select  dt
      |                                ,song_id
      |                                ,play_rank
      |                                ,effective_play_cnt_1d_avg_l7d  --7日均有效播放次数
      |                        from    ads_itm_pgc_song_muse_tag_dd
      |                        where   dt = '2022-01-01'
      |                        ) b
      |                on      a.dt = b.dt
      |                and     a.song_id = b.song_id
      |                group by a.dt
      |                        ,a.playlist_id
      |                ) a
      |        ) d
      |on      a.dt = d.dt
      |and     a.playlist_id = d.playlist_id
      |left join (--复播
      |        select  dt
      |                ,playlist_id
      |                ,dau_vs_mau
      |        from    tmp_ads_itm_muse_playlist_dau_vs_mau_di
      |        where   dt = '2022-01-01'
      |        ) e
      |on      a.dt = e.dt
      |and     a.playlist_id = e.playlist_id
      |left join (--歌单中歌曲播放集中度
      |        select  dt
      |                ,playlist_id
      |                ,top5_non_author_effective_play_cnt_28d / 28 as list_top5_song_effective_play_cnt_28d_avg
      |                ,top25_non_author_effective_play_cnt_28d / 28 as list_top25_song_effective_play_cnt_28d_avg
      |                ,if(non_author_effective_play_cnt_28d = 0, 0, top5_non_author_effective_play_cnt_28d / non_author_effective_play_cnt_28d) as list_top5_song_effective_play_pct_28d
      |                ,if(non_author_effective_play_cnt_28d = 0, 0, top25_non_author_effective_play_cnt_28d / non_author_effective_play_cnt_28d) as list_top25_song_effective_play_pct_28d
      |        from    tmp_ads_itm_muse_playlist_song_dist_28d
      |        where   dt = '2022-01-01'
      |        ) f
      |on      a.dt = f.dt
      |and     a.playlist_id = f.playlist_id
      |left join (--歌单中用户集中度
      |        select  dt
      |                ,playlist_id
      |                ,top5_non_author_effective_play_cnt_28d / 28 as list_top5_user_effective_play_cnt_28d_avg
      |                ,top25_non_author_effective_play_cnt_28d / 28 as list_top25_user_effective_play_cnt_28d_avg
      |                ,if(non_author_effective_play_cnt_28d = 0, 0, top5_non_author_effective_play_cnt_28d / non_author_effective_play_cnt_28d) as list_top5_user_effective_play_pct_28d
      |                ,if(non_author_effective_play_cnt_28d = 0, 0, top25_non_author_effective_play_cnt_28d / non_author_effective_play_cnt_28d) as list_top25_user_effective_play_pct_28d
      |        from    tmp_ads_itm_muse_playlist_user_dist_28d
      |        where   dt = '2022-01-01'
      |        ) g
      |on      a.dt = g.dt
      |and     a.playlist_id = g.playlist_id
      |left join (
      |        select  dt
      |                ,playlist_id
      |                ,top5_effective_play_cnt_7d / 7 as app_top5_song_effective_play_cnt_7d_avg
      |                ,top25_effective_play_cnt_7d / 28 as app_top25_song_effective_play_cnt_7d_avg
      |                ,if(effective_play_cnt_7d = 0, 0, top5_effective_play_cnt_7d / effective_play_cnt_7d) as app_top5_song_effective_play_pct_7d
      |                ,if(effective_play_cnt_7d = 0, 0, top25_effective_play_cnt_7d / effective_play_cnt_7d) as app_top25_song_effective_play_pct_7d
      |        from    tmp_ads_itm_muse_app_song_dist_7d
      |        where   dt = '2022-01-01'
      |        ) h
      |on      a.dt = h.dt
      |and     a.playlist_id = h.playlist_id
      |left join (--歌单创建人信息
      |        select  dt
      |                ,playlist_id
      |                ,user_id
      |                ,effective_play_song_num_28d
      |                ,effective_play_cnt_28d
      |                ,effective_play_duration_28d
      |                ,db_collect_playlist_num_std
      |                ,db_followed_unt_std
      |                ,collected_playlist_num
      |                ,collected_playlist_cnt
      |        from    tmp_ads_itm_muse_playlist_creator_tag_dd
      |        where   dt = '2022-01-01'
      |        ) i
      |on      a.dt = i.dt
      |and     a.playlist_id = i.playlist_id
      |left join (--歌单曝光点击
      |        select  dt
      |                ,playlist_id
      |                --7d
      |                ,exp_cnt_7d
      |                ,initiative_exp_cnt_7d
      |                ,alg_exp_cnt_7d
      |                ,operate_exp_cnt_7d
      |                ,other_exp_cnt_7d
      |                ,if(exp_cnt_7d = 0, 0, initiative_exp_cnt_7d / exp_cnt_7d) as initiative_exp_pct_7d
      |                ,if(exp_cnt_7d = 0, 0, alg_exp_cnt_7d / exp_cnt_7d) as alg_exp_pct_7d
      |                ,if(exp_cnt_7d = 0, 0, operate_exp_cnt_7d / exp_cnt_7d) as operate_exp_pct_7d
      |                ,if(exp_cnt_7d = 0, 0, other_exp_cnt_7d / exp_cnt_7d) as other_exp_pct_7d
      |                ,exp_cnt_7d / 7 as exp_cnt_7d_avg
      |                ,initiative_exp_cnt_7d / 7 as initiative_exp_cnt_7d_avg
      |                ,alg_exp_cnt_7d / 7 as alg_exp_cnt_7d_avg
      |                ,operate_exp_cnt_7d / 7 as operate_exp_cnt_7d_avg
      |                ,other_exp_cnt_7d / 7 as other_exp_cnt_7d_avg
      |                ,clk_cnt_7d
      |                ,initiative_clk_cnt_7d
      |                ,alg_clk_cnt_7d
      |                ,operate_clk_cnt_7d
      |                ,other_clk_cnt_7d
      |                ,if(clk_cnt_7d = 0, 0, initiative_clk_cnt_7d / clk_cnt_7d) as initiative_clk_pct_7d
      |                ,if(clk_cnt_7d = 0, 0, alg_clk_cnt_7d / clk_cnt_7d) as alg_clk_pct_7d
      |                ,if(clk_cnt_7d = 0, 0, operate_clk_cnt_7d / clk_cnt_7d) as operate_clk_pct_7d
      |                ,if(clk_cnt_7d = 0, 0, other_clk_cnt_7d / clk_cnt_7d) as other_clk_pct_7d
      |                ,clk_cnt_7d / 7 as clk_cnt_7d_avg
      |                ,initiative_clk_cnt_7d / 7 as initiative_clk_cnt_7d_avg
      |                ,alg_clk_cnt_7d / 7 as alg_clk_cnt_7d_avg
      |                ,operate_clk_cnt_7d / 7 as operate_clk_cnt_7d_avg
      |                ,other_clk_cnt_7d / 7 as other_clk_cnt_7d_avg
      |                ,if(exp_cnt_7d = 0, 0, clk_cnt_7d / exp_cnt_7d) as cnt_ctr_7d
      |                ,if(initiative_exp_cnt_7d = 0, 0, initiative_clk_cnt_7d / initiative_exp_cnt_7d) as initiative_cnt_ctr_7d
      |                ,if(alg_exp_cnt_7d = 0, 0, alg_clk_cnt_7d / alg_exp_cnt_7d) as alg_cnt_ctr_7d
      |                ,if(operate_exp_cnt_7d = 0, 0, operate_clk_cnt_7d / operate_exp_cnt_7d) as operate_cnt_ctr_7d
      |                ,if(other_exp_cnt_7d = 0, 0, other_clk_cnt_7d / other_exp_cnt_7d) as other_cnt_ctr_7d
      |                ,exp_cnt_28d
      |                ,initiative_exp_cnt_28d
      |                ,alg_exp_cnt_28d
      |                ,operate_exp_cnt_28d
      |                ,other_exp_cnt_28d
      |                ,if(exp_cnt_28d = 0, 0, initiative_exp_cnt_28d / exp_cnt_28d) as initiative_exp_pct_28d
      |                ,if(exp_cnt_28d = 0, 0, alg_exp_cnt_28d / exp_cnt_28d) as alg_exp_pct_28d
      |                ,if(exp_cnt_28d = 0, 0, operate_exp_cnt_28d / exp_cnt_28d) as operate_exp_pct_28d
      |                ,if(exp_cnt_28d = 0, 0, other_exp_cnt_28d / exp_cnt_28d) as other_exp_pct_28d
      |                ,exp_cnt_28d / 28 as exp_cnt_28d_avg
      |                ,initiative_exp_cnt_28d / 28 as initiative_exp_cnt_28d_avg
      |                ,alg_exp_cnt_28d / 28 as alg_exp_cnt_28d_avg
      |                ,operate_exp_cnt_28d / 28 as operate_exp_cnt_28d_avg
      |                ,other_exp_cnt_28d / 28 as other_exp_cnt_28d_avg
      |                ,clk_cnt_28d
      |                ,initiative_clk_cnt_28d
      |                ,alg_clk_cnt_28d
      |                ,operate_clk_cnt_28d
      |                ,other_clk_cnt_28d
      |                ,if(clk_cnt_28d = 0, 0, initiative_clk_cnt_28d / clk_cnt_28d) as initiative_clk_pct_28d
      |                ,if(clk_cnt_28d = 0, 0, alg_clk_cnt_28d / clk_cnt_28d) as alg_clk_pct_28d
      |                ,if(clk_cnt_28d = 0, 0, operate_clk_cnt_28d / clk_cnt_28d) as operate_clk_pct_28d
      |                ,if(clk_cnt_28d = 0, 0, other_clk_cnt_28d / clk_cnt_28d) as other_clk_pct_28d
      |                ,clk_cnt_28d / 28 as clk_cnt_28d_avg
      |                ,initiative_clk_cnt_28d / 28 as initiative_clk_cnt_28d_avg
      |                ,alg_clk_cnt_28d / 28 as alg_clk_cnt_28d_avg
      |                ,operate_clk_cnt_28d / 28 as operate_clk_cnt_28d_avg
      |                ,other_clk_cnt_28d / 28 as other_clk_cnt_28d_avg
      |                ,if(exp_cnt_28d = 0, 0, clk_cnt_28d / exp_cnt_28d) as cnt_ctr_28d
      |                ,if(initiative_exp_cnt_28d = 0, 0, initiative_clk_cnt_28d / initiative_exp_cnt_28d) as initiative_cnt_ctr_28d
      |                ,if(alg_exp_cnt_28d = 0, 0, alg_clk_cnt_28d / alg_exp_cnt_28d) as alg_cnt_ctr_28d
      |                ,if(operate_exp_cnt_28d = 0, 0, operate_clk_cnt_28d / operate_exp_cnt_28d) as operate_cnt_ctr_28d
      |                ,if(other_exp_cnt_28d = 0, 0, other_clk_cnt_28d / other_exp_cnt_28d) as other_cnt_ctr_28d
      |        from    tmp_ads_itm_muse_playlist_log_tag_28d
      |        where   dt = '2022-01-01'
      |        ) j
      |on      a.dt = j.dt
      |and     a.playlist_id = j.playlist_id
      |left join (--口径调整，先换成数仓的表
      |        --待调整
      |        select  dt
      |                ,source_id
      |                ,count(userid) as first_play_unt_1d
      |                ,sum(if(user_active_type = '1', 1, 0)) as first_song_play_new_unt_1d
      |                ,sum(if(user_active_type = '2', 1, 0)) as first_song_play_recall_unt_1d
      |                ,sum(if(user_active_type = '3', 1, 0)) as first_song_play_retain_unt_1d
      |        from    (
      |                select  dt
      |                        ,cast(resource_id as string) as song_id
      |                        ,cast(is_fake as string) as is_fake
      |                        ,case   when from_unixtime(first_log_time, 'yyyy-MM-dd') = '2022-01-01' then '1'
      |                                when datediff(dt, second_last_log_date) > 30 then '2'  --30日召回
      |                                when datediff(dt, second_last_log_date) <= 30 then '3'  --留存
      |                                when first_log_time is null then '4'  -- 黑名单
      |                                else '5'  -- 其他，没有second_last_log_date的数据
      |                                end as user_active_type
      |                        ,cast(userid as string) as userid
      |                        ,cast(source_id as string) as source_id
      |                from    user_first_play_day
      |                where   dt = '2022-01-01'
      |                and     resource_type = 'song'
      |                and     refer not in ('banner', 'dailySongRecommend', 'userfm')
      |                and     ban = 0
      |                and     is_fake = 0  --剔除作弊流量
      |                ) a
      |        group by dt
      |                ,source_id
      |        ) k
      |on      a.dt = k.dt
      |and     a.playlist_id = k.source_id
      |left join (--歌单中歌曲播放人数
      |        select  dt
      |                ,playlist_id
      |                ,sum(if(song_play_cnt_1d > 0, 1, 0)) as non_author_song_play_unt_1d
      |                ,sum(if(song_effective_play_cnt_1d > 0, 1, 0)) as non_author_song_effective_play_unt_1d
      |                ,sum(if(song_full_play_cnt_1d > 0, 1, 0)) as non_author_song_full_play_unt_1d
      |                ,sum(if(song_like_cnt_1d > 0, 1, 0)) as non_author_song_like_unt_1d
      |                ,sum(if(song_play_cnt_7d > 0, 1, 0)) as non_author_song_play_unt_7d
      |                ,sum(if(song_effective_play_cnt_7d > 0, 1, 0)) as non_author_song_effective_play_unt_7d
      |                ,sum(if(song_full_play_cnt_7d > 0, 1, 0)) as non_author_song_full_play_unt_7d
      |                ,sum(if(song_like_cnt_7d > 0, 1, 0)) as non_author_song_like_unt_7d
      |                ,sum(if(song_play_cnt_28d > 0, 1, 0)) as non_author_song_play_unt_28d
      |                ,sum(if(song_effective_play_cnt_28d > 0, 1, 0)) as non_author_song_effective_play_unt_28d
      |                ,sum(if(song_full_play_cnt_28d > 0, 1, 0)) as non_author_song_full_play_unt_28d
      |                ,sum(if(song_like_cnt_28d > 0, 1, 0)) as non_author_song_like_unt_28d
      |                ,if(sum(song_effective_play_cnt_1d) = 0, 0, sum(effective_play_song_num_1d) / sum(if(song_effective_play_cnt_1d > 0, 1, 0))) as non_author_effective_play_song_num_1d_user_avg
      |                ,if(sum(song_full_play_cnt_1d) = 0, 0, sum(full_play_song_num_1d) / sum(if(song_full_play_cnt_1d > 0, 1, 0))) as non_author_full_play_song_num_1d_user_avg
      |        from    tmp_ads_itm_muse_playlist_user_tag_summary
      |        where   dt = '2022-01-01'
      |        and     is_playlist_author = 0 --剔除歌单创建人
      |        group by dt
      |                ,playlist_id
      |        ) l
      |on      a.dt = l.dt
      |and     a.playlist_id = l.playlist_id
      |left join (
      |        select  dt
      |                ,playlist_id
      |                ,count(1) as main_language_genre_tag_num
      |                ,concat_ws(',', collect_set(concat(standard_language, '&', genre_tag_name, '-', round(pct, 2)))) as main_language_genre_tag_list
      |        from    tmp_ads_itm_muse_playlist_main_genre_tag_dd
      |        where   dt = '2022-01-01'
      |        group by dt
      |                ,playlist_id
      |        ) m
      |on      a.dt = m.dt
      |and     a.playlist_id = m.playlist_id
      |""".stripMargin
}