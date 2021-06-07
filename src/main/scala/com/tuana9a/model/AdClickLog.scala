package com.tuana9a.model

import java.sql.Timestamp

case class AdClickLog(timestamp: Timestamp, //EXPLAIN: thời gian ghi log
                      campaign_id: String,
                      banner_id: String,
                      domain: String,
                      zone_id: String,
                      path: String,
                      click_or_view: String,
                      encode_bid: String,
                      type_ad: String,
                      ssp_zone: String,
                      dt: String, //EXPLAIN: thời gian user click
                      uid: String,
                      ip: String,
                      click_id: String,
                      geo_id: String)
