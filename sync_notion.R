#!/usr/bin/env Rscript

# --- packages ---
need <- c("httr2","jsonlite","DBI","RPostgres")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")
invisible(lapply(need, library, character.only = TRUE))

# --- config ---
NOTION_TOKEN <- Sys.getenv("NOTION_TOKEN")
DB_ID        <- Sys.getenv("NOTION_DATABASE_ID") # hyphenated UUID
if (!nzchar(NOTION_TOKEN) || !nzchar(DB_ID)) stop("Set NOTION_TOKEN and NOTION_DATABASE_ID.")
LOOKBACK_HOURS <- as.integer(Sys.getenv("LOOKBACK_HOURS", "24"))
NOTION_VERSION <- "2022-06-28"

# --- helpers ---
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x
rtxt <- function(x) {
  s <- as.character(x %||% "")
  if (identical(s, "")) list()
  else list(list(type="text", text=list(content=substr(s, 1, 1800))))
}
nnum  <- function(x) suppressWarnings(if (is.null(x) || is.na(x)) NULL else as.numeric(x))
nbool <- function(x) isTRUE(x)
ndate <- function(x) {
  if (is.null(x) || is.na(x)) return(NULL)
  list(start = format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ"))
}

props_from_row <- function(r) {
  list(
    tweet_id         = list(title     = rtxt(r$tweet_id)),
    tweet_url        = list(url       = r$tweet_url %||% NULL),
    username         = list(rich_text = rtxt(r$username)),
    user_id          = list(rich_text = rtxt(r$user_id)),
    text             = list(rich_text = rtxt(r$text)),
    reply_count      = list(number    = nnum(r$reply_count)),
    retweet_count    = list(number    = nnum(r$retweet_count)),
    like_count       = list(number    = nnum(r$like_count)),
    quote_count      = list(number    = nnum(r$quote_count)),
    bookmarked_count = list(number    = nnum(r$bookmarked_count)),
    view_count       = list(number    = nnum(r$view_count)),  # handles integer64 -> double
    date             = list(date      = ndate(r$date)),
    is_quote         = list(checkbox  = nbool(r$is_quote)),
    is_retweet       = list(checkbox  = nbool(r$is_retweet)),
    engagement_rate  = list(number    = nnum(r$engagement_rate))
  )
}

notion_req <- function(url) {
  request(url) |>
    req_headers(
      Authorization   = paste("Bearer", NOTION_TOKEN),
      "Notion-Version"= NOTION_VERSION,
      "Content-Type"  = "application/json"
    )
}

find_page_by_tweet_id <- function(tweet_id) {
  body <- list(
    filter = list(property = "tweet_id", title = list(equals = as.character(tweet_id))),
    page_size = 1
  )
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")) |>
    req_body_json(body) |> req_perform()
  out <- resp_body_json(resp, simplifyVector = TRUE)
  if (length(out$results)) out$results$id[1] else NA_character_
}

create_page <- function(pr) {
  body <- list(parent = list(database_id = DB_ID), properties = pr)
  resp <- notion_req("https://api.notion.com/v1/pages") |>
    req_body_json(body) |> req_perform()
  resp_body_json(resp, simplifyVector = TRUE)$id
}

update_page <- function(page_id, pr) {
  notion_req(paste0("https://api.notion.com/v1/pages/", page_id)) |>
    req_method("PATCH") |>
    req_body_json(list(properties = pr)) |>
    req_perform()
  invisible(TRUE)
}

upsert_row <- function(r) {
  pr  <- props_from_row(r)
  pid <- tryCatch(find_page_by_tweet_id(r$tweet_id), error = function(e) NA_character_)
  if (is.na(pid)) create_page(pr) else update_page(pid, pr)
}

# --- Supabase (Postgres) ---
supa_host <- Sys.getenv("SUPABASE_HOST")
supa_user <- Sys.getenv("SUPABASE_USER")
supa_pwd  <- Sys.getenv("SUPABASE_PWD")
if (!nzchar(supa_host) || !nzchar(supa_user) || !nzchar(supa_pwd)) {
  stop("Set SUPABASE_HOST, SUPABASE_USER, SUPABASE_PWD.")
}

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = supa_host,
  port = as.integer(Sys.getenv("SUPABASE_PORT", "5432")),
  dbname = as.character(Sys.getenv("SUPABASE_DB", "postgres")),
  user = supa_user,
  password = supa_pwd,
  sslmode = "require"
)

since <- as.POSIXct(Sys.time(), tz = "UTC") - LOOKBACK_HOURS*3600
since_str <- format(since, "%Y-%m-%d %H:%M:%S%z")

qry <- sprintf("
  SELECT
    tweet_id, tweet_url, username, user_id, text,
    reply_count, retweet_count, like_count, quote_count, bookmarked_count,
    view_count, date, is_quote, is_retweet, engagement_rate
  FROM twitter_raw
  WHERE date >= TIMESTAMPTZ '%s'
  ORDER BY date ASC
", since_str)

rows <- DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

message(sprintf("Fetched %d rows since %s", nrow(rows), since_str))

# --- Upsert into Notion ---
if (nrow(rows)) {
  for (i in seq_len(nrow(rows))) {
    r <- rows[i, , drop = FALSE]
    try(upsert_row(r), silent = TRUE)
    if (i %% 10 == 0) message(sprintf("Upserted %d/%d", i, nrow(rows)))
    Sys.sleep(0.35)  # ~3 rps limit
  }
  message("Done.")
} else {
  message("Nothing to sync.")
}
