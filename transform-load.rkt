#lang racket

(require db)
(require srfi/19) ; Time Data Types and Procedures
(require threading)

(struct dividend-entry
  (ex-date
   amount))

(struct split-entry
  (date
   ratio))

(display "dividends splits base folder [/var/tmp/yahoo/dividends-splits]: ")
(flush-output)
(define dividends-splits-base-folder
  (let ([dividends-splits-base-folder-input (read-line)])
    (if (equal? "" dividends-splits-base-folder-input) "/var/tmp/yahoo/dividends-splits"
        dividends-splits-base-folder-input)))

(display (string-append "dividends splits folder date [" (date->string (current-date) "~1") "]: "))
(flush-output)
(define folder-date
  (let ([date-string-input (read-line)])
    (if (equal? "" date-string-input) (current-date)
        (string->date date-string-input "~Y-~m-~d"))))

(display (string-append "db user [user]: "))
(flush-output)
(define db-user
  (let ([db-user-input (read-line)])
    (if (equal? "" db-user-input) "user"
        db-user-input)))

(display (string-append "db name [local]: "))
(flush-output)
(define db-name
  (let ([db-name-input (read-line)])
    (if (equal? "" db-name-input) "local"
        db-name-input)))

(display (string-append "db pass []: "))
(flush-output)
(define db-pass (read-line))

(define dbc (postgresql-connect #:user db-user #:database db-name #:password db-pass))

(parameterize ([current-directory (string-append dividends-splits-base-folder "/" (date->string folder-date "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) "-div.csv")) (in-directory))])
    (let ([file-name (string-append dividends-splits-base-folder "/" (date->string folder-date "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) "-div.csv" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-symbol
                                                                      " for date "
                                                                      (date->string folder-date "~1")))
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

(parameterize ([current-directory (string-append dividends-splits-base-folder "/" (date->string folder-date "~1") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) "-split.csv")) (in-directory))])
    (let ([file-name (string-append dividends-splits-base-folder "/" (date->string folder-date "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) "-split.csv" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-symbol
                                                                      " for date "
                                                                      (date->string folder-date "~1")))
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