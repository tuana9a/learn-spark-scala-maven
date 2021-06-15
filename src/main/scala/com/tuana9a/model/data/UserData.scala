package com.tuana9a.model.data

import java.sql.Timestamp
import java.lang.Double

case class UserData(registration_dttm: Timestamp = null,
                    id: Int = 0,
                    first_name: String = null,
                    last_name: String = null,
                    email: String = null,
                    gender: String = null,
                    ip_address: String = null,
                    cc: String = null,
                    country: String = null,
                    birthdate: String = null,
                    salary: Double = 0.0,
                    title: String = null,
                    comments: String = null)
