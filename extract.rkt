#lang racket

(require db)
(require net/url)
(require srfi/19) ; Time Data Types and Procedures
(require tasks)
(require threading)

(define unix-epoch (date->time-utc (string->date "1970-01-01T00:00:00Z+0" "~Y-~m-~dT~H:~M:~SZ~z")))

(define (download-history symbol start-time end-time div-or-split crumb cookie)
  (make-directory* (string-append "/var/tmp/yahoo/dividends-splits/" (date->string (current-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/yahoo/dividends-splits/" (date->string (current-date) "~1") "/"
                                        (string-replace symbol  "-" ".") "-" div-or-split ".csv")
    (λ (out)
      (~> (string-append "https://query1.finance.yahoo.com/v7/finance/download/" symbol "?period1=" start-time "&period2=" end-time
                         "&interval=1d&events=" div-or-split "&crumb=" crumb)
          (string->url _)
          (get-pure-port _ (list (string-append "cookie: B=" cookie ";")))
          (copy-port _ out)))
    #:exists 'replace))

(display (string-append "start date [" (date->string (current-date) "~1") "]: "))
(flush-output)
(define start-time
  (let ([date-string-input (read-line)])
    (if (equal? "" date-string-input) (number->string (time-second (time-difference (date->time-utc (current-date))
                                                                                    unix-epoch)))
        (number->string (time-second (time-difference (date->time-utc (string->date date-string-input "~Y-~m-~d"))
                                                      unix-epoch))))))

(display (string-append "end date [" (date->string (current-date) "~1") "]: "))
(flush-output)
(define end-time
  (let ([date-string-input (read-line)])
    (if (equal? "" date-string-input) (number->string (time-second (time-difference (date->time-utc (current-date))
                                                                                    unix-epoch)))
        (number->string (time-second (time-difference (date->time-utc (string->date date-string-input "~Y-~m-~d"))
                                                      unix-epoch))))))

(display (string-append "crumb []: "))
(flush-output)
(define crumb (read-line))

(display (string-append "cookie []: "))
(flush-output)
(define cookie (read-line))

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

(define symbols (query-list dbc "
select
  replace(act_symbol, '.', '-') as act_symbol
from
  nasdaq.symbol
where
  is_test_issue = false and
  is_next_shares = false and
  nasdaq_symbol !~ '[-\\$\\+\\*#!@%\\^=~]' and
  case when nasdaq_symbol ~ '[A-Z]{4}[L-Z]'
    then security_name !~ '(Note|Preferred|Right|Unit|Warrant)'
    else true
  end
order by
  act_symbol;
"))

(disconnect dbc)

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-history (first l) start-time end-time "div" crumb cookie)
                                                            (download-history (first l) start-time end-time "split" crumb cookie))
                                                          (second l)))
                            (map list symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
