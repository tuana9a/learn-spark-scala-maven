package com.tuana9a.model.data

case class PageViewLog(dt: String,
                       cookietime: String,
                       browser_code: Int,
                       browser_ver: String,
                       os_code: Int,
                       os_version: String,
                       ip: Long,
                       loc_id: Int,
                       domain: String,
                       siteId: String,
                       channelId: String,
                       path: String,
                       referer: String,
                       guid: String,
                       geo: Int,
                       org_ip: String,
                       pageloadId: String,
                       screen: String,
                       d_guid: String,
                       category: String,
                       utm_source: String,
                       utm_campaign: String,
                       utm_medium: String,
                       milis: Long)
