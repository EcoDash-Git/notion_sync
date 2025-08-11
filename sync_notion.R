#!/usr/bin/env Rscript

# --- packages ---
need <- c("httr2","jsonlite","DBI","RPostgres")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")
invisible(lapply(need, library, character.only = TRUE))

# --- config ---
NOTION_TOKEN   <- Sys.getenv("NOTION_TOKEN")
DB_ID          <- Sys.getenv("NOTION_DATABASE_ID") # hyphenated UUID
LOOKBACK_HOURS <- as.integer(Sys.getenv("LOOKBACK_HOURS", "24"))
NOTION_VERSION <- "2022-06-28"

if (!nzchar(NOTION_TOKEN) || !nzchar(DB_ID)) {
  stop("Set NOTION_TOKEN and NOTION_DATABASE_ID.")
}

# --- helpers ---
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x

rtxt <- function(x) {
  s <- as.character(x %||% "")
  if (identical(s, "")) list() else list(list(type = "text", text = list(content = substr(s, 1, 1800))))
}

# Safer perform() that prints real Notion errors but never calls resp_body_* on NULL
perform <- function(req, tag = "") {
  tryCatch({
    req_perform(req)
  }, error = function(e) {
    cat("\n--- Notion request failed", if (nzchar(tag)) paste0(" [", tag, "]"), "---\n")
    if (inherits(e, "httr2_http") && !is.null(e$response)) {
      sc <- e$response$status_code
      body <- tryCatch(resp_body_string(e$response), error = function(...) "")
      cat("Status:", sc, "\nBody:  ", body, "\n")
    } else {
      cat("Message:", conditionMessage(e), "\n")
    }
    cat("---------------------------------------------\n")
    return(NULL)
  })
}

notion_req <- function(url) {
  request(url) |>
    req_headers(
      Authorization    = paste("Bearer", NOTION_TOKEN),
      "Notion-Version" = NOTION_VERSION,
      "Content-Type"   = "application/json"
    )
}

# --- read DB schema & detect the Title property name ---
get_db_schema <- function() {
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID)) |> perform(tag = "GET /databases")
  if (is.null(resp)) stop("Could not read Notion database schema (check token, share, and DB_ID).")
  resp_body_json(resp, simplifyVector = FALSE)
}

.DB <- get_db_schema()
PROPS <- .DB$properties
TITLE_PROP <- names(Filter(function(p) identical(p$type, "title"), PROPS))[1]
if (is.null(TITLE_PROP)) stop("This Notion database has no Title property.")

# Build a property payload that ONLY includes columns that exist in Notion
set_prop <- function(name, value) {
  p <- PROPS[[name]]
  if (is.null(p)) return(NULL)
  tp <- p$type
  if (tp == "title") {
    list(title = list(list(type="text", text=list(content = as.character(value %||% "untitled")))))
  } else if (tp == "rich_text") {
    list(rich_text = rtxt(value))
  } else if (tp == "number") {
    list(number = suppressWarnings(as.numeric(value)))
  } else if (tp == "url") {
    list(url = as.character(value %||% NULL))
  } else if (tp == "date") {
    if (is.null(value) || is.na(value)) list(date = NULL) else
      list(date = list(start = format(as.POSIXct(value, tz="UTC"), "%Y-%m-%dT%H:%M:%SZ")))
  } else if (tp == "checkbox") {
    list(checkbox = isTRUE(value))
  } else {
    NULL
  }
}

props_from_row <- function(r) {
  pr <- list()
  # Title: use tweet_id if present, otherwise any non-empty text
  title_val <- r$tweet_id %||% r$text %||% "untitled"
  pr[[TITLE_PROP]] <- set_prop(TITLE_PROP, title_val)

  # Optional extras – only included if the column exists in Notion
  for (nm in c("tweet_id","username","user_id","text","tweet_url","reply_count",
               "retweet_count","like_count","quote_count","bookmarked_count",
               "view_count","date","is_quote","is_retweet","engagement_rate")) {
    if (nm == TITLE_PROP) next
    if (!is.null(PROPS[[nm]]) && !is.null(r[[nm]])) {
      pr[[nm]] <- set_prop(nm, r[[nm]])
    }
  }
  pr
}

find_page_by_title_eq <- function(val) {
  body <- list(filter = list(property = TITLE_PROP, title = list(equals = as.character(val %||% ""))),
               page_size = 1)
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")) |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag = "POST /databases/query")
  if (is.null(resp)) return(NA_character_)
  out <- resp_body_json(resp, simplifyVector = TRUE)
  if (length(out$results)) out$results$id[1] else NA_character_
}

create_page <- function(pr) {
  body <- list(parent = list(database_id = DB_ID), properties = pr)
  resp <- notion_req("https://api.notion.com/v1/pages") |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag = "POST /pages")
  if (is.null(resp)) return(NA_character_)
  resp_body_json(resp, simplifyVector = TRUE)$id
}

update_page <- function(page_id, pr) {
  resp <- notion_req(paste0("https://api.notion.com/v1/pages/", page_id)) |>
    req_method("PATCH") |>
    req_body_json(list(properties = pr), auto_unbox = TRUE) |>
    perform(tag = "PATCH /pages/:id")
  !is.null(resp)
}

upsert_row <- function(r) {
  pr <- props_from_row(r)
  title_val <- r$tweet_id %||% r$text %||% "untitled"
  pid <- tryCatch(find_page_by_title_eq(title_val), error = function(e) NA_character_)
  if (is.na(pid)) {
    !is.na(create_page(pr))
  } else {
    update_page(pid, pr)
  }
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

since <- as.POSIXct(Sys.time(), tz = "UTC") - LOOKBACK_HOURS * 3600
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
success <- 0L
if (nrow(rows)) {
  for (i in seq_len(nrow(rows))) {
    r <- rows[i, , drop = FALSE]
    ok <- FALSE
    try({ ok <- isTRUE(upsert_row(r)) }, silent = TRUE)
    if (ok) success <- success + 1L else
      message(sprintf("Row %d failed (tweet_id=%s)", i, as.character(r$tweet_id)))
    if (i %% 10 == 0) message(sprintf("Processed %d/%d (success %d)", i, nrow(rows), success))
    Sys.sleep(0.35)  # ~3 rps limit
  }
  message(sprintf("Done. %d/%d succeeded.", success, nrow(rows)))
} else {
  message("Nothing to sync.")
}
