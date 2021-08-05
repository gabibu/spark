package entities


case class Booking(hotel:String, is_canceled:Boolean,
                   lead_time:Long,  arrival_date_year:Long,
                   arrival_date_month:String, arrival_date_week_number:Long,
                   arrival_date_day_of_month:Long,
                   stays_in_weekend_nights:Long, stays_in_week_nights:Long,
                   adults:Long, children:Float,
                   babies:Long,
                   meal:String, country:String,
                   market_segment:String, distribution_channel:String,
                   is_repeated_guest:Long,
                   previous_cancellations:Long,
                   previous_bookings_not_canceled:Long,
                   reserved_room_type:String,
                   assigned_room_type:String,
                   booking_changes:Long,
                   deposit_type:String,
                   agent:Float,
                   company:Float,
                   days_in_waiting_list:Long,
                   customer_type:String, adr:Float,
                   required_car_parking_spaces:Long,
                   total_of_special_requests:Long,
                   reservation_status:String,
                   reservation_status_date:String
                  )

