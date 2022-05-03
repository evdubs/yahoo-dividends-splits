#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/list
         racket/sequence
         racket/string
         threading)

(struct dividend-entry
  (ex-date
   amount))

(struct split-entry
  (date
   ratio))

(define base-folder (make-parameter "/var/tmp/yahoo/dividends-splits"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Yahoo dividends and splits base folder. Defaults to /var/tmp/yahoo/dividends-splits"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "Yahoo dividends and splits folder date. Defaults to today"
                         (folder-date (iso8601->date date))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) "-div.csv")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [ticker-symbol (string-replace (string-replace file-name (path->string (current-directory)) "") "-div.csv" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-symbol
                                                                      " for date "
                                                                      (~t (folder-date) "yyyy-MM-dd")))
                                       (displayln e)
                                       (rollback-transaction dbc))])
            (~> (in-lines in)
                (sequence-map (λ (el) (string-split el ",")) _)
                (sequence-filter (λ (el) (not (equal? "Date" (first el)))) _)
                (sequence-for-each (λ (el)
                                     (start-transaction dbc)
                                     (let ([div (apply dividend-entry el)])
                                       (query-exec dbc "
insert into yahoo.dividend (
  act_symbol,
  ex_date,
  amount
) values (
  $1,
  $2::text::date,
  $3::text::numeric
) on conflict (act_symbol, ex_date) do nothing;
"
                                                   ticker-symbol
                                                   (dividend-entry-ex-date div)
                                                   (dividend-entry-amount div))
                                       (commit-transaction dbc))) _))))))))

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) "-split.csv")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [ticker-symbol (string-replace (string-replace file-name (path->string (current-directory)) "") "-split.csv" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-symbol
                                                                      " for date "
                                                                      (~t (folder-date) "yyyy-MM-dd")))
                                       (displayln e)
                                       (rollback-transaction dbc))])
            (~> (in-lines in)
                (sequence-map (λ (el) (string-split el ",")) _)
                (sequence-filter (λ (el) (not (equal? "Date" (first el)))) _)
                (sequence-for-each (λ (el)
                                     (start-transaction dbc)
                                     (let ([split (apply split-entry el)])
                                       (query-exec dbc "
insert into yahoo.stock_split (
  act_symbol,
  date,
  new_share_amount,
  old_share_amount
) values (
  $1,
  $2::text::date,
  $3::text::integer,
  $4::text::integer
) on conflict (act_symbol, date) do nothing;
"
                                                   ticker-symbol
                                                   (split-entry-date split)
                                                   (if (string-contains? (split-entry-ratio split) "/")
                                                       (first (string-split (split-entry-ratio split) "/"))
                                                       (second (string-split (split-entry-ratio split) ":")))
                                                   (if (string-contains? (split-entry-ratio split) "/")
                                                       (second (string-split (split-entry-ratio split) "/"))
                                                       (first (string-split (split-entry-ratio split) ":"))))
                                       (commit-transaction dbc))) _))))))))

(disconnect dbc)
