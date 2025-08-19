#!/usr/bin/env Rscript

# ─────────────────────────────────────────────────────────────────────────────
# Sync Supabase (Postgres) → Notion database (chunked, with retries & preindex)
# ─────────────────────────────────────────────────────────────────────────────

# --- packages ----------------------------------------------------------------
need <- c("httr2","jsonlite","DBI","RPostgres")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")
invisible(lapply(need, library, character.only = TRUE))

# --- config ------------------------------------------------------------------
NOTION_TOKEN     <- Sys.getenv("NOTION_TOKEN")
DB_ID            <- Sys.getenv("NOTION_DATABASE_ID")  # hyphenated UUID
LOOKBACK_HOURS   <- as.integer(Sys.getenv("LOOKBACK_HOURS", "17532")) # ~2y
NOTION_VERSION   <- "2022-06-28"

# Optional knobs (via env or edit here)
NUM_NA_AS_ZERO   <- tolower(Sys.getenv("NUM_NA_AS_ZERO","false")) %in% c("1","true","yes")
INSPECT_FIRST_ROW<- tolower(Sys.getenv("INSPECT_FIRST_ROW","false")) %in% c("1","true","yes")
DUMP_SCHEMA      <- tolower(Sys.getenv("DUMP_SCHEMA","false"))       %in% c("1","true","yes")
RATE_DELAY_SEC   <- as.numeric(Sys.getenv("RATE_DELAY_SEC","0.2"))   # was 0.35
IMPORT_ALL       <- tolower(Sys.getenv("IMPORT_ALL","false"))        %in% c("1","true","yes")
RUN_SMOKE_TEST   <- tolower(Sys.getenv("RUN_SMOKE_TEST","false"))    %in% c("1","true","yes")
CHUNK_SIZE       <- as.integer(Sys.getenv("CHUNK_SIZE","800"))       # rows per run
CHUNK_OFFSET     <- as.integer(Sys.getenv("CHUNK_OFFSET","0"))       # skip rows

if (!nzchar(NOTION_TOKEN) || !nzchar(DB_ID)) stop("Set NOTION_TOKEN and NOTION_DATABASE_ID.")

# --- helpers -----------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x
rtxt <- function(x) {
  s <- as.character(x %||% "")
  if (identical(s, "")) list() else list(list(type="text", text=list(content=substr(s, 1, 1800))))
}

# robust datetime coercion (returns POSIXct(NA) if it can’t parse)
parse_dt <- function(x) {
  if (inherits(x, "POSIXt")) return(as.POSIXct(x, tz="UTC"))
  if (inherits(x, "Date"))   return(as.POSIXct(x, tz="UTC"))
  if (inherits(x, "integer64")) x <- as.character(x)
  if (is.numeric(x))         return(as.POSIXct(x, origin="1970-01-01", tz="UTC"))
  if (!is.character(x))      return(as.POSIXct(NA, origin="1970-01-01", tz="UTC"))
  xx <- trimws(x)
  fmts <- c("%Y-%m-%dT%H:%M:%OSZ",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S %z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d")
  for (f in fmts) {
    d <- suppressWarnings(as.POSIXct(xx, format=f, tz="UTC"))
    if (!is.na(d)) return(d)
  }
  as.POSIXct(NA, origin="1970-01-01", tz="UTC")
}

# perform() with retry/backoff (handles 429/5xx and network glitches)
perform <- function(req, tag = "", max_tries = 6, base_sleep = 0.5) {
  last <- NULL
  for (i in seq_len(max_tries)) {
    resp <- tryCatch(req_perform(req), error = function(e) e)
    last <- resp
    if (inherits(resp, "httr2_response")) {
      sc <- resp$status_code
      if (sc != 429 && sc < 500) return(resp)  # success or non-retryable
      ra <- resp_headers(resp)[["retry-after"]]
      wait <- if (!is.null(ra)) suppressWarnings(as.numeric(ra)) else base_sleep * 2^(i - 1)
      Sys.sleep(min(wait, 10))
    } else {
      Sys.sleep(base_sleep * 2^(i - 1))  # network/other error
    }
  }
  err <- list(
    tag = tag,
    status = if (inherits(last, "httr2_response")) last$status_code else NA_integer_,
    body = if (inherits(last, "httr2_response")) tryCatch(resp_body_string(last), error = function(...) "<no body>")
           else paste("R error:", conditionMessage(last))
  )
  structure(list(.err = TRUE, err = err), class = "notion_err")
}
is_err  <- function(x) inherits(x, "notion_err") && isTRUE(x$.err %||% TRUE)  # tolerant
show_err <- function(x, row_i = NA, tweet_id = NA) {
  if (!is_err(x)) return(invisible())
  er <- x$err
  cat(sprintf("⚠️ Notion error%s%s [%s] Status: %s\nBody: %s\n",
              if (!is.na(row_i)) paste0(" on row ", row_i) else "",
              if (!is.na(tweet_id)) paste0(" (tweet_id=", tweet_id, ")") else "",
              er$tag %||% "request",
              as.character(er$status %||% "n/a"),
              er$body %||% "<empty>"))
}

notion_req <- function(url) {
  request(url) |>
    req_headers(
      Authorization    = paste("Bearer", NOTION_TOKEN),
      "Notion-Version" = NOTION_VERSION,
      "Content-Type"   = "application/json"
    )
}

# --- read DB schema & detect the Title property name -------------------------
get_db_schema <- function() {
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID)) |> perform(tag="GET /databases")
  if (is_err(resp)) {
    show_err(resp)
    stop("Could not read Notion database schema (check token, DB_ID, and share the database with the integration).")
  }
  resp_body_json(resp, simplifyVector = FALSE)
}

.DB <- get_db_schema()
PROPS <- .DB$properties
TITLE_PROP <- names(Filter(function(p) identical(p$type, "title"), PROPS))[1]
if (is.null(TITLE_PROP)) stop("This Notion database has no Title (Name) property.")

if (DUMP_SCHEMA) {
  cat("\n--- Notion schema ---\n")
  cat(paste(
    vapply(names(PROPS), function(n) sprintf("%s : %s", n, PROPS[[n]]$type), character(1)),
    collapse = "\n"
  ), "\n----------------------\n")
}

# coercers for number/checkbox robustness
to_bool <- function(x) {
  if (is.logical(x)) return(isTRUE(x))
  if (is.numeric(x)) return(is.finite(x) && x != 0)
  if (is.character(x)) return(tolower(trimws(x)) %in% c("true","t","yes","1"))
  FALSE
}
to_num <- function(x) {
  if (inherits(x, "integer64")) x <- as.character(x)
  v <- suppressWarnings(as.numeric(x))
  if (is.na(v) && NUM_NA_AS_ZERO) v <- 0
  v
}

# Build a property payload that ONLY includes columns that exist in Notion
set_prop <- function(name, value) {
  p <- PROPS[[name]]
  if (is.null(p)) return(NULL)
  tp <- p$type
  if (tp == "title") {
    list(title = list(list(type="text", text=list(content=as.character(value %||% "untitled")))))
  } else if (tp == "rich_text") {
    list(rich_text = rtxt(value))
  } else if (tp == "number") {
    v <- to_num(value); if (!is.finite(v)) return(NULL)
    list(number = v)
  } else if (tp == "url") {
    u <- as.character(value %||% ""); if (!nzchar(u)) return(list(url = NULL))
    list(url = u)
  } else if (tp == "date") {
    d <- parse_dt(value); if (is.na(d)) return(NULL)
    list(date = list(start = format(d, "%Y-%m-%dT%H:%M:%SZ")))
  } else if (tp == "checkbox") {
    list(checkbox = to_bool(value))
  } else {
    NULL
  }
}

props_from_row <- function(r) {
  pr <- list()
  # Title: prefer tweet_id; fall back to tweet text
  title_val <- r$tweet_id %||% r$text %||% "untitled"
  pr[[TITLE_PROP]] <- set_prop(TITLE_PROP, title_val)
  # Optional extras – only if the column exists in Notion
  for (nm in c("username","user_id","text","tweet_url","reply_count",
               "retweet_count","like_count","quote_count","bookmarked_count",
               "view_count","date","is_quote","is_retweet","engagement_rate","tweet_id")) {
    if (!is.null(PROPS[[nm]]) && !is.null(r[[nm]])) {
      pr[[nm]] <- set_prop(nm, r[[nm]])
    }
  }
  pr
}

# Inspector: prints mapping decisions for a single row
explain_props <- function(r) {
  wanted <- c("username","user_id","text","tweet_url","reply_count",
              "retweet_count","like_count","quote_count","bookmarked_count",
              "view_count","date","is_quote","is_retweet","engagement_rate","tweet_id")
  cat("\n--- Property mapping (one row) ---\n")
  for (nm in wanted) {
    exists <- !is.null(PROPS[[nm]])
    tp <- if (exists) PROPS[[nm]]$type else "<missing>"
    val <- r[[nm]]
    payload <- if (exists) set_prop(nm, val) else NULL
    cat(sprintf("%-17s | notion=%-9s | R=%-10s | value=%s | %s\n",
      nm, tp, paste(class(val), collapse="+"),
      if (length(val)) paste0(utils::head(as.character(val),1)) else "<NULL>",
      if (is.null(payload)) "SKIPPED" else "OK"))
  }
}

# Legacy per-row title query (kept as fallback)
find_page_by_title_eq <- function(val) {
  body <- list(filter = list(property = TITLE_PROP, title = list(equals = as.character(val %||% ""))),
               page_size = 1)
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")) |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag="POST /databases/query")
  if (is_err(resp)) return(structure(NA_character_, class="notion_err", .err=TRUE, err=resp$err))
  out <- resp_body_json(resp, simplifyVector = TRUE)
  if (length(out$results)) out$results$id[1] else NA_character_
}

# --- PRE-INDEX: read the whole Notion DB once, map title & tweet_id ----------
has_prop <- function(nm) !is.null(PROPS[[nm]])

fetch_notion_index <- function() {
  url <- paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")
  cursor <- NULL
  by_title <- new.env(parent = emptyenv())
  by_tid   <- new.env(parent = emptyenv())
  n <- 0L

  repeat {
    body <- list(page_size = 100)
    if (!is.null(cursor)) body$start_cursor <- cursor
    resp <- notion_req(url) |> req_body_json(body, auto_unbox = TRUE) |> perform(tag = "QUERY ALL")
    if (is_err(resp)) { show_err(resp); break }
    out <- resp_body_json(resp, simplifyVector = FALSE)

    for (pg in out$results) {
      pid <- pg$id
      # Title text (concatenate fragments)
      tnodes <- pg$properties[[TITLE_PROP]]$title
      ttl <- if (length(tnodes)) paste0(vapply(tnodes, \(x) x$plain_text %||% "", character(1L)), collapse = "") else ""
      if (nzchar(ttl)) by_title[[ttl]] <- pid

      # tweet_id if present (rich_text recommended)
      if (!is.null(PROPS$tweet_id) && identical(PROPS$tweet_id$type, "rich_text")) {
        rt <- pg$properties$tweet_id$rich_text
        tid <- if (length(rt)) paste0(vapply(rt, \(x) x$plain_text %||% "", character(1L)), collapse = "") else ""
        if (nzchar(tid)) by_tid[[tid]] <- pid
      }
      n <- n + 1L
    }
    if (isTRUE(out$has_more)) cursor <- out$next_cursor else break
  }
  message(sprintf("Indexed %d Notion pages (title & tweet_id maps).", n))
  list(by_title = by_title, by_tid = by_tid)
}

# --- CRUD wrappers -----------------------------------------------------------
create_page <- function(pr) {
  body <- list(parent = list(database_id = DB_ID), properties = pr)
  resp <- notion_req("https://api.notion.com/v1/pages") |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag="POST /pages")
  if (is_err(resp)) return(structure(NA_character_, class="notion_err", .err=TRUE, err=resp$err))
  resp_body_json(resp, simplifyVector = TRUE)$id
}

update_page <- function(page_id, pr) {
  resp <- notion_req(paste0("https://api.notion.com/v1/pages/", page_id)) |>
    req_method("PATCH") |>
    req_body_json(list(properties = pr), auto_unbox = TRUE) |>
    perform(tag="PATCH /pages/:id")
  if (is_err(resp)) return(structure(FALSE, class="notion_err", .err=TRUE, err=resp$err))
  TRUE
}

# Upsert using pre-index (fast); falls back to per-row title query if no index
upsert_row <- function(r, idx = NULL) {
  title_val <- as.character(r$tweet_id %||% r$text %||% "untitled")
  pr_full   <- props_from_row(r)

  # Resolve page id via pre-index maps
  pid <- NA_character_
  if (!is.null(idx)) {
    tid_chr <- as.character(r$tweet_id %||% "")
    if (!is.null(PROPS$tweet_id) && nzchar(tid_chr)) {
      pid <- idx$by_tid[[tid_chr]]
      if (is.null(pid)) pid <- NA_character_
    }
    if (is.na(pid) || is.null(pid)) {
      pid <- idx$by_title[[title_val]]
      if (is.null(pid)) pid <- NA_character_
    }
  } else {
    pid <- find_page_by_title_eq(title_val)  # slower fallback
  }

  # Update or create
  if (!is.na(pid[1])) {
    ok <- update_page(pid, pr_full)
    return(is.logical(ok) && ok)
  }

  pid2 <- create_page(pr_full)
  if (!is.na(pid2[1])) {
    # warm the index for subsequent rows (if available)
    if (!is.null(idx)) {
      idx$by_title[[title_val]] <- pid2
      tid_chr <- as.character(r$tweet_id %||% "")
      if (!is.null(PROPS$tweet_id) && nzchar(tid_chr)) idx$by_tid[[tid_chr]] <- pid2
    }
    return(TRUE)
  }

  # Minimal create then patch (last resort)
  pr_min <- list(); pr_min[[TITLE_PROP]] <- set_prop(TITLE_PROP, title_val)
  pid3 <- create_page(pr_min)
  if (is.na(pid3[1])) return(FALSE)

  ok2 <- update_page(pid3, pr_full)
  is.logical(ok2) && ok2
}

# --- Supabase (Postgres) -----------------------------------------------------
supa_host <- Sys.getenv("SUPABASE_HOST")
supa_user <- Sys.getenv("SUPABASE_USER")
supa_pwd  <- Sys.getenv("SUPABASE_PWD")
if (!nzchar(supa_host) || !nzchar(supa_user) || !nzchar(supa_pwd)) stop("Set SUPABASE_HOST, SUPABASE_USER, SUPABASE_PWD.")

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

if (IMPORT_ALL) {
  qry <- sprintf("
    SELECT
      tweet_id, tweet_url, username, user_id, text,
      reply_count, retweet_count, like_count, quote_count, bookmarked_count,
      view_count, date, is_quote, is_retweet, engagement_rate
    FROM twitter_raw
    ORDER BY date ASC
    LIMIT %d OFFSET %d
  ", CHUNK_SIZE, CHUNK_OFFSET)
} else {
  qry <- sprintf("
    SELECT
      tweet_id, tweet_url, username, user_id, text,
      reply_count, retweet_count, like_count, quote_count, bookmarked_count,
      view_count, date, is_quote, is_retweet, engagement_rate
    FROM twitter_raw
    WHERE date >= TIMESTAMPTZ '%s'
    ORDER BY date ASC
    LIMIT %d OFFSET %d
  ", since_str, CHUNK_SIZE, CHUNK_OFFSET)
}

rows <- DBI::dbGetQuery(con, qry)
DBI::dbDisconnect(con)

message(sprintf("Fetched %d rows (%s). CHUNK_SIZE=%d OFFSET=%d",
                nrow(rows), if (IMPORT_ALL) "ALL" else paste("since", since_str),
                CHUNK_SIZE, CHUNK_OFFSET))

# --- Optional smoke test (skip during big backfills) -------------------------
if (RUN_SMOKE_TEST) {
  smoke_title <- paste0("ping ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
  smoke_pid <- create_page(setNames(list(set_prop(TITLE_PROP, smoke_title)), TITLE_PROP))
  if (is_err(smoke_pid) || is.na(smoke_pid[1])) {
    cat("\n🔥 Smoke test FAILED — cannot create even a minimal page in this database.\n",
        "Confirm the integration is connected to THIS database and has insert/update permissions.\n", sep = "")
    if (is_err(smoke_pid)) show_err(smoke_pid)
    quit(status = 1L, save = "no")
  } else {
    notion_req(paste0("https://api.notion.com/v1/pages/", smoke_pid)) |>
      req_method("PATCH") |>
      req_body_json(list(archived = TRUE), auto_unbox = TRUE) |>
      perform(tag="ARCHIVE ping")
  }
}

# Optional one-row inspector
if (INSPECT_FIRST_ROW && nrow(rows)) explain_props(rows[1, , drop = FALSE])

# --- Build Notion index once (big speed-up) ----------------------------------
idx <- fetch_notion_index()

# --- Upsert into Notion (chunk) ---------------------------------------------
success <- 0L
if (nrow(rows)) {
  for (i in seq_len(nrow(rows))) {
    r  <- rows[i, , drop = FALSE]
    # Guard: if 'date' is unparseable, drop it for this row
    if (!is.null(r$date)) {
      d <- parse_dt(r$date); if (is.na(d)) r$date <- NULL else r$date <- d
    }
    ok <- FALSE
    ok <- upsert_row(r, idx = idx)
    if (ok) success <- success + 1L else message(sprintf("Row %d failed (tweet_id=%s)", i, as.character(r$tweet_id)))
    if (i %% 50 == 0) message(sprintf("Processed %d/%d (success %d)", i, nrow(rows), success))
    Sys.sleep(RATE_DELAY_SEC)  # stay under Notion’s rps with retries to absorb 429s
  }
  message(sprintf("Done. %d/%d succeeded in this chunk.", success, nrow(rows)))
} else {
  message("Nothing to sync in this chunk.")
}




