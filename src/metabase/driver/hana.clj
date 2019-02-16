(ns metabase.driver.hana
  "SAP Hana driver. Builds off of the Generic SQL driver."
  (:require [clj-time
             [coerce :as tcoerce]
             [core :as t]
             [format :as time]]
            [clojure
             [set :as set]
             [string :as str]]
            [honeysql.core :as hsql]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.db.spec :as dbspec]
            [metabase.driver.generic-sql :as sql]
            [metabase.driver.generic-sql.query-processor :as sqlqp]
            [metabase.util
             [date :as du]
             [honeysql-extensions :as hx]
             [ssh :as ssh]]
            [schema.core :as s])
  (:import java.sql.Time
           [java.util Date TimeZone]
           metabase.util.honeysql_extensions.Literal
           org.joda.time.format.DateTimeFormatter))

(defrecord HanaDriver []
  :load-ns true
  clojure.lang.Named
  (getName [_] "Hana"))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  METHOD IMPLS                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- column->base-type [column-type]
  ({:ALPHANUM   :type/Text
    :BIGINT     :type/BigInteger
    :BLOB       :type/*
    :CLOB       :type/Text
    :DATE       :type/Date
    :DECIMAL    :type/Decimal
    :DOUBLE     :type/Float
    :INTEGER    :type/Integer
    :NCLOB      :type/Text
    :NVARCHAR   :type/Text
    :REAL       :type/Float
    :SECONDDATE :type/DateTime
    :SMALLDECIMAL    :type/Decimal
    :SMALLINT   :type/Integer
    :SHORTTEXT  :type/Text
    :TEXT       :type/Text
    :TIME       :type/Time
    :TIMESTAMP  :type/DateTime
    :TINYINT    :type/Integer
    :VARBINARY  :type/*
    :VARCHAR    :type/Text} (keyword (str/replace (name column-type) #"\sUNSIGNED$" "")))) ; strip off " UNSIGNED" from end if present

(def ^:private ^:const default-connection-args
  "Map of args for the MySQL JDBC connection string.
   Full list of is options is available here: http://dev.mysql.com/doc/connector-j/6.0/en/connector-j-reference-configuration-properties.html"
  {;; 0000-00-00 dates are valid in MySQL; convert these to `null` when they come back because they're illegal in Java
   :emptyTimestampIsNull          :true})

(def ^:private ^:const ^String default-connection-args-string
  (str/join \& (for [[k v] default-connection-args]
                 (str (name k) \= (name v)))))

(defn- append-connection-args
  "Append `default-connection-args-string` to the connection string in CONNECTION-DETAILS, and an additional option to
  explicitly disable SSL if appropriate. (Newer versions of MySQL will complain if you don't explicitly disable SSL.)"
  {:argslist '([connection-spec details])}
  [connection-spec {ssl? :ssl}]
  (update connection-spec :subname
          (fn [subname]
            (let [join-char (if (str/includes? subname "?") "&" "?")]
              (str subname join-char default-connection-args-string (when-not ssl?
                                                                      "&useSSL=false"))))))

(defn- connection-details->spec [details]
  (-> details
      (set/rename-keys {:dbname :db})
      dbspec/mysql
      (append-connection-args details)
      (sql/handle-additional-options details)))

(defn- date-format [format-str expr] (hsql/call :to_char expr (hx/literal format-str)))
(defn- str-to-date [format-str expr] (hsql/call :to_date expr (hx/literal format-str)))

(defmethod sqlqp/->honeysql [HanaDriver Time]
  [_ time-value]
  (hx/->time time-value))

;; Since MySQL doesn't have date_trunc() we fake it by formatting a date to an appropriate string and then converting
;; back to a date. See http://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format for an
;; explanation of format specifiers
(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defn- date [unit expr]
  (case unit
    :default         expr
    :minute          (hsql/call :minute expr)
    :minute-of-hour  (hx/minute expr)
    :hour            (hsql/call :hour expr)
    :hour-of-day     (hx/hour expr)
    :day             (hsql/call :to_date expr)
    :day-of-week     (hsql/call :dayofweek expr)
    :day-of-month    (hsql/call :dayofmonth expr)
    :day-of-year     (hsql/call :dayofyear expr)
    ;; To convert a YEARWEEK (e.g. 201530) back to a date you need tell MySQL which day of the week to use,
    ;; because otherwise as far as MySQL is concerned you could be talking about any of the days in that week
    :week            (hx/concat (hsql/call :week expr)
                                (hsql/call :year expr))
    ;; mode 6: Sunday is first day of week, first week of year is the first one with 4+ days
    :week-of-year    (hsql/call :week expr)
    :month           (str-to-date "YYYY-MM-DD"
                                  (hx/concat (date-format "YYYY-MM" expr)
                                             (hx/literal "-01")))
    :month-of-year   (hx/month expr)
    ;; Truncating to a quarter is trickier since there aren't any format strings.
    ;; See the explanation in the H2 driver, which does the same thing but with slightly different syntax.
    :quarter         (str-to-date "YYYY-MM-DD"
                                  (hx/concat (hx/year expr)
                                             (hx/literal "-")
                                             (hx/- (hx/* (hx/quarter expr)
                                                         3)
                                                   2)
                                             (hx/literal "-01")))
                                      
    :quarter-of-year (hx/quarter expr)
    :year            (hsql/call :year expr)))
                        
(defn- humanize-connection-error-message [message]
  (condp re-matches message
        #"^Communications link failure\s+The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.$"
        (driver/connection-error-messages :cannot-connect-check-host-and-port)

        #"^Unknown database .*$"
        (driver/connection-error-messages :database-name-incorrect)

        #"Access denied for user.*$"
        (driver/connection-error-messages :username-or-password-incorrect)

        #"Must specify port after ':' in connection string"
        (driver/connection-error-messages :invalid-hostname)

        #".*" ; default
        message))

(defn- string-length-fn [field-key]
  (hsql/call :length field-key))

(def ^:private ^:const now             (hsql/raw "CURRENT_TIMESTAMP"))
(def ^:private ^:const date-1970-01-01 (hsql/call :to_timestamp (hx/literal :1970-01-01) (hx/literal :YYYY-MM-DD)))

(defn- num-to-ds-interval [unit amount] (hsql/call :interval amount (hx/literal unit)))

(defn- date-interval [unit amount]
  (hx/+
    now
    (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))

(defn- unix-timestamp->timestamp [field-or-value seconds-or-milliseconds]
  (hx/+ date-1970-01-01 (num-to-ds-interval :second (case seconds-or-milliseconds
                                                      :seconds      field-or-value
                                                      :milliseconds (hx// field-or-value (hsql/raw 1000))))))


(def ^:private hana-date-formatters (driver/create-db-time-formatters "yyyy-MM-dd HH:mm:ss.SSS zzz"))
(def ^:private hana-db-time-query "select to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF3 TZD') FROM DUMMY")

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                        IDRIVER & ISQLDRIVER METHOD MAPS                                        |
;;; +----------------------------------------------------------------------------------------------------------------+

(u/strict-extend HanaDriver
  driver/IDriver
  (merge
   (sql/IDriverSQLDefaultsMixin)
   {:date-interval                     (u/drop-first-arg date-interval)
    :details-fields                    (constantly (ssh/with-tunnel-config
                                                     [driver/default-host-details
                                                      (assoc driver/default-port-details :default 30015)
                                                      driver/default-dbname-details
                                                      driver/default-user-details
                                                      driver/default-password-details
                                                      (assoc driver/default-additional-options-details
                                                        :placeholder  "tinyInt1isBit=false")]))
    :humanize-connection-error-message (u/drop-first-arg humanize-connection-error-message)
    :current-db-time                   (driver/make-current-db-time-fn hana-db-time-query hana-date-formatters)})

  sql/ISQLDriver
  (merge
   (sql/ISQLDriverDefaultsMixin)
   {:active-tables             sql/post-filtered-active-tables
    :column->base-type         (u/drop-first-arg column->base-type)
    :connection-details->spec  (u/drop-first-arg connection-details->spec)
    :date                      (u/drop-first-arg date)
    :current-datetime-fn       (constantly now)
    :excluded-schemas          (constantly #{"INFORMATION_SCHEMA"})
    ;; TODO
    :quote-style               (constantly :mysql)
    :string-length-fn          (u/drop-first-arg string-length-fn)
    ;; TODO - This can also be set via `sessionVariables` in the connection string, if that's more useful (?)
    :set-timezone-sql          (constantly "SET @@session.time_zone = %s;")
    :unix-timestamp->timestamp (u/drop-first-arg unix-timestamp->timestamp)}))

(defn -init-driver
  "Register the SAP Hana driver"
  []
  (driver/register-driver! :hana (HanaDriver.)))
