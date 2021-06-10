package com.tuana9a.model.data

import java.sql.Timestamp

case class UserData(registration_dttm: Timestamp,
                    id: Int,
                    first_name: String,
                    last_name: String,
                    email: String,
                    gender: String,
                    ip_address: String,
                    cc: String,
                    country: String,
                    birthdate: String,
                    salary: Double,
                    title: String,
                    comments: String)
