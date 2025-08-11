#!/usr/bin/env Rscript

# --- packages ---
need <- c("httr2","jsonlite","DBI","RPostgres")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")
invisible(lapply(need, library, character.only = TRUE))

# --- config ---
NOTION_TOKEN   <- Sys.getenv("NOTION_TOKEN")
DB_ID          <- Sys.getenv("NOTION_DATABASE_ID") # hyphenated UUID of the *database*
LOOKBACK_HOURS <- as.integer(Sys.getenv("LOOKBACK_HOURS", "24"))
NOTION_VERSION <- "2022-06-28"

if (!nzchar(NOTION_TOKEN) || !nzchar(DB_ID)) stop("Set NOTION_TOKEN and NOTION_DATABASE_ID.")

# --- helpers ---
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x
rtxt <- function(x) {
  s <- as.character(x %||% "")
  if (identical(s, "")) list() else list(list(type = "text", text = list(content = substr(s, 1, 1800))))
}

# perform() that NEVER throws; returns either the response or NULL with a rich error attribute
perform <- function(req, tag = "") {
  tryCatch({
    req_perform(req)
  }, error = function(e) {
    err <- list(tag = tag, status = NA_integer_, body = paste("R error:", conditionMessage(e)))
    if (inherits(e, "httr2_http") && !is.null(e$response)) {
      err$status <- e$response$status_code
      err$body   <- tryCatch(resp_body_string(e$response), error = function(...) "<no body>")
    }
    structure(NULL, err = err)
  })
}
is_err <- function(x) is.null(x) && !is.null(attr(x, "err"))
show_err <- function(x, row_i = NA, tweet_id = NA) {
  er <- attr(x, "err")
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

# --- read DB schema & detect the Title property name ---
get_db_schema <- function() {
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID)) |> perform(tag = "GET /databases")
  if (is_err(resp)) {
    show_err(resp)
    stop("Could not read Notion database schema (check token, DB_ID, and that the integration is shared to this database).")
  }
  resp_body_json(resp, simplifyVector = FALSE)
}

.DB <- get_db_schema()
PROPS <- .DB$properties
TITLE_PROP <- names(Filter(function(p) identical(p$type, "title"), PROPS))[1]
if (is.null(TITLE_PROP)) stop("This Notion database has no Title property (Name).")

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
    v <- suppressWarnings(as.numeric(value))
    if (!is.finite(v)) return(NULL)  # skip impossible numbers
    list(number = v)
  } else if (tp == "url") {
    list(url = as.character(value %||% NULL))
  } else if (tp == "date") {
    if (is.null(value) || is.na(value)) {
      list(date = NULL)
    } else {
      list(date = list(start = format(as.POSIXct(value, tz="UTC"), "%Y-%m-%dT%H:%M:%SZ")))
    }
  } else if (tp == "checkbox") {
    list(checkbox = isTRUE(value))
  } else {
    NULL
  }
}

props_from_row <- function(r) {
  pr <- list()
  # Title: prefer tweet_id text; fall back to tweet text
  title_val <- r$tweet_id %||% r$text %||% "untitled"
  pr[[TITLE_PROP]] <- set_prop(TITLE_PROP, title_val)

  # Optional extras – only if column exists in Notion
  # NOTE: We deliberately skip 'tweet_id' here to avoid number/precision pitfalls; it's stored in the Title.
  for (nm in c("username","user_id","text","tweet_url","reply_count",
               "retweet_count","like_count","quote_count","bookmarked_count",
               "view_count","date","is_quote","is_retweet","engagement_rate")) {
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
  if (is_err(resp)) return(structure(NA_character_, err = attr(resp, "err")))
  out <- resp_body_json(resp, simplifyVector = TRUE)
  if (length(out$results)) out$results$id[1] else NA_character_
}

create_page <- function(pr) {
  body <- list(parent = list(database_id = DB_ID), properties = pr)
  resp <- notion_req("https://api.notion.com/v1/pages") |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag = "POST /pages")
  if (is_err(resp)) return(structure(NA_character_, err = attr(resp, "err")))
  resp_body_json(resp, simplifyVector = TRUE)$id
}

update_page <- function(page_id, pr) {
  resp <- notion_req(paste0("https://api.notion.com/v1/pages/", page_id)) |>
    req_method("PATCH") |>
    req_body_json(list(properties = pr), auto_unbox = TRUE) |>
    perform(tag = "PATCH /pages/:id")
  if (is_err(resp)) return(structure(FALSE, err = attr(resp, "err")))
  TRUE
}

# Try full upsert; on create failure, try minimal (Title-only) then patch
upsert_row <- function(r, i) {
  title_val <- r$tweet_id %||% r$text %||% "untitled"
  pr_full   <- props_from_row(r)

  pid <- find_page_by_title_eq(title_val)
  if (!is.na(pid[1])) {
    ok <- update_page(pid, pr_full)
    if (is.logical(ok) && ok) return(list(ok=TRUE))
    return(list(ok=FALSE, err=attr(ok, "err")))
  }

  # create
  pid2 <- create_page(pr_full)
  if (!is.na(pid2[1])) return(list(ok=TRUE))

  # fallback: minimal title then patch
  pr_min <- list()
  pr_min[[TITLE_PROP]] <- set_prop(TITLE_PROP, title_val)
  pid3 <- create_page(pr_min)
  if (is.na(pid3[1])) return(list(ok=FALSE, err=attr(pid2, "err")))  # report first create error

  ok2 <- update_page(pid3, pr_full)
  if (is.logical(ok2) && ok2) return(list(ok=TRUE))
  list(ok=FALSE, err=attr(ok2, "err"))
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

# --- SMOKE TEST: can we write a trivial row? ---
smoke_title <- paste0("ping ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
smoke_pid <- create_page(setNames(list(set_prop(TITLE_PROP, smoke_title)), TITLE_PROP))
if (is.na(smoke_pid[1])) {
  cat("\n🔥 Smoke test FAILED — cannot create even a minimal page in this database.\n",
      "Please confirm: the integration is connected to THIS database (not just the page), ",
      "and has insert/update permissions. Also re-check NOTION_DATABASE_ID.\n", sep = "")
  # also print the detailed error:
  show_err(smoke_pid)
  quit(status = 1L, save = "no")
} else {
  # tidy up: delete the ping page (soft delete)
  notion_req(paste0("https://api.notion.com/v1/blocks/", smoke_pid)) |>
    req_method("PATCH") |>
    req_body_json(list(archived = TRUE), auto_unbox = TRUE) |>
    perform(tag = "ARCHIVE ping")
}

# --- Upsert into Notion ---
success <- 0L
if (nrow(rows)) {
  for (i in seq_len(nrow(rows))) {
    r  <- rows[i, , drop = FALSE]
    res <- upsert_row(r, i)
    if (isTRUE(res$ok)) {
      success <- success + 1L
    } else {
      show_err(structure(NULL, err = res$err %||% list(tag="unknown",status=NA,body="<no details>")),
               row_i = i, tweet_id = r$tweet_id)
    }
    if (i %% 10 == 0) message(sprintf("Processed %d/%d (success %d)", i, nrow(rows), success))
    Sys.sleep(0.35)  # ~3 rps limit
  }
  message(sprintf("Done. %d/%d succeeded.", success, nrow(rows)))
} else {
  message("Nothing to sync.")
}

