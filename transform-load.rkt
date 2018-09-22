#lang racket/base

(require db
         racket/cmdline
         racket/list
         racket/sequence
         racket/string
         srfi/19 ; Time Data Types and Procedures
         threading)

(struct dividend-entry
  (ex-date
   amount))

(struct split-entry
  (date
   ratio))

(define base-folder (make-parameter "/var/tmp/yahoo/dividends-splits"))

(define folder-date (make-parameter (current-date)))

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
                         (folder-date (string->date date "~Y-~m-~d"))]
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

(parameterize ([current-directory (string-append (base-folder) "/" (date->string (folder-date) "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) "-div.csv")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) "-div.csv" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-symbol
                                                                      " for date "
                                                                      (date->string (folder-date) "~1")))
                                       (displayln ((error-value->string-handler) e 1000))
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

(parameterize ([current-directory (string-append (base-folder) "/" (date->string (folder-date) "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) "-split.csv")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) "-split.csv" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-symbol
                                                                      " for date "
                                                                      (date->string (folder-date) "~1")))
                                       (displayln ((error-value->string-handler) e 1000))
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
                                                   (first (string-split (split-entry-ratio split) "/"))
                                                   (second (string-split (split-entry-ratio split) "/")))
                                       (commit-transaction dbc))) _))))))))

(disconnect dbc)