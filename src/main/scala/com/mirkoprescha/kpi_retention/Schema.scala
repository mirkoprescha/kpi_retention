package com.mirkoprescha.kpi_retention

import java.sql.Timestamp

import org.apache.spark.sql.types.{TimestampType, DateType}

case class User(
                 user_id: String
                 , created_on_ts: java.sql.Timestamp
                 , common_name: String
                 , user_type_cd: String
                 , gender_cd: String
                 , country_cd: String
               )


case class Event(
                  event_ts: Timestamp
                  , request_uri: String
                  , session_id: String
                  , principal: String
                )


case class UserEvent(
                      user_id: String
                      , created_on_ts: Timestamp
                      , common_name: String
                      , user_type_cd: String
                      , gender_cd: String
                      , country_cd: String
                      , event_ts: Timestamp
                      , request_uri: String
                      , session_id: String
                      , principal: String
                    )


case class Retention(
                      retention_period_in_days: Int
                      , first_game: String
                      //,gender_cd                       char(2)
                      //,country_cd                      char(2)
                      //,number_of_registrations        bigint
                      , number_of_returning_players: Int
                      , returning_pct: Double
                    )