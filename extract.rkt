#lang racket/base

(require db
         gregor
         gregor/period
         net/http-easy
         racket/cmdline
         racket/file
         racket/list
         racket/port
         racket/string
         tasks
         threading)

(define unix-epoch (moment 1970 1 1 0 0 0 0 #:tz "UTC"))

(define (download-history symbol start-time end-time div-or-split crumb cookie)
  (make-directory* (string-append "/var/tmp/yahoo/dividends-splits/" (date->iso8601 (today))))
  (call-with-output-file* (string-append "/var/tmp/yahoo/dividends-splits/" (date->iso8601 (today)) "/"
                                         (string-replace symbol  "-" ".") "-" div-or-split ".csv")
    (λ (out) (with-handlers ([exn:fail?
                              (λ (error)
                                (displayln (string-append "Encountered error for " symbol))
                                (displayln ((error-value->string-handler) error 1000)))])
               (~> (string-append "https://query1.finance.yahoo.com/v7/finance/download/" symbol "?period1=" start-time "&period2=" end-time
                                  "&interval=1d&events=" div-or-split "&crumb=" crumb)
                   (get _ #:headers (hash 'cookie (string-append "B=" cookie)))
                   (response-body _)
                   (write-bytes _ out))))
    #:exists 'replace))

(define start-time (make-parameter (number->string (period-ref (period-between unix-epoch (now/moment) '(seconds)) 'seconds))))

(define end-time (make-parameter (number->string (period-ref (period-between unix-epoch (now/moment) '(seconds)) 'seconds))))

(define crumb (make-parameter ""))

(define cookie (make-parameter ""))

(define first-symbol (make-parameter ""))

(define last-symbol (make-parameter ""))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket extract.rkt"
 #:once-each
 [("-c" "--cookie") c
                    "Cookie"
                    (cookie c)]
 [("-e" "--end-date") end
                      "Final date for history retrieval. Defaults to today"
                      (end-time (number->string (period-ref (period-between unix-epoch (parse-moment end "yyyy-MM-dd") '(seconds)) 'seconds)))]
 [("-f" "--first-symbol") first
                          "First symbol to query. Defaults to nothing"
                          (first-symbol first)]
 [("-l" "--last-symbol") last
                         "Last symbol to query. Defaults to nothing"
                         (last-symbol last)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-r" "--crumb") r
                   "Crumb"
                   (crumb r)]
 [("-s" "--start-date") start
                        "Earliest date for history retrieval. Defaults to today"
                        (start-time (number->string (period-ref (period-between unix-epoch (parse-moment start "yyyy-MM-dd") '(seconds)) 'seconds)))]
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
  end and
  last_seen = (select max(last_seen) from nasdaq.symbol) and
  case when $1 != ''
    then act_symbol >= $1
    else true
  end and
  case when $2 != ''
    then act_symbol <= $2
    else true
  end
order by
  act_symbol;
"
                            (first-symbol)
                            (last-symbol)))

(disconnect dbc)

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (thread (λ () (download-history (first l) (start-time) (end-time)
                                                                                                "split" (crumb) (cookie)))))
                                                          (second l))
                               (schedule-delayed-task (λ () (thread (λ () (download-history (first l) (start-time) (end-time)
                                                                                            "div" (crumb) (cookie)))))
                                                      (+ 5 (second l))))
                            (map list symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
