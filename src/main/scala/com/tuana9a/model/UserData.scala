package com.tuana9a.model

import java.sql.Timestamp

case class UserData(registration_dttm: Timestamp,
                    id: Long,
                    first_name: String,
                    last_name: String,
                    email: String,
                    gender: String,
                    ip_address: String,
                    cc: String,
                    country: String,
                    birthdate: String,
                    salary: String,
                    title: String,
                    comments: String)
