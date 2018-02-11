#lang racket

(require db)
(require net/url)
(require racket/cmdline)
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

(define start-time (make-parameter (number->string (time-second (time-difference (date->time-utc (current-date)) unix-epoch)))))

(define end-time (make-parameter (number->string (time-second (time-difference (date->time-utc (current-date)) unix-epoch)))))

(define crumb (make-parameter ""))

(define cookie (make-parameter ""))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket extract.rkt"
 #:once-each
 [("-ck" "--cookie") ck
                     "Cookie"
                     (cookie ck)]
 [("-cr" "--crumb") cr
                    "Crumb"
                    (crumb cr)]
 [("-e" "--end-date") end
                      "Final date for history retrieval. Defaults to today"
                      (end-time (number->string (time-second (time-difference (date->time-utc (string->date end "~Y-~m-~d"))
                                                                              unix-epoch))))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-s" "--start-date") start
                        "Earliest date for history retrieval. Defaults to today"
                        (start-time (number->string (time-second (time-difference (date->time-utc (string->date start "~Y-~m-~d"))
                                                                                  unix-epoch))))]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

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

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-history (first l) (start-time) (end-time) "div" (crumb) (cookie))
                                                            (download-history (first l) (start-time) (end-time) "split" (crumb) (cookie)))
                                                          (second l)))
                            (map list symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
