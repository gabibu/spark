package entities

import java.sql.Timestamp


case class ListeningRow(user_id: String, timestamp: Timestamp,
              artist_id: String,
              artist_name: String,
              track_id: String,
              track_name: String)
