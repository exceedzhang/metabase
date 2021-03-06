(ns metabase.test.data.dataset-definitions
  "Definitions of various datasets for use in tests with `with-temp-db`."
  (:require [metabase.test.data.interface :as di])
  (:import java.sql.Time
           [java.util Calendar TimeZone]))

;; ## Datasets

;; The O.G. "Test Database" dataset
(di/def-database-definition-edn test-data)

;; Times when the Toucan cried
(di/def-database-definition-edn sad-toucan-incidents)

;; Places, times, and circumstances where Tupac was sighted
(di/def-database-definition-edn tupac-sightings)

(di/def-database-definition-edn geographical-tips)

;; A very tiny dataset with a list of places and a booleans
(di/def-database-definition-edn places-cam-likes)

;; A small dataset with users and a set of messages between them. Each message has *2* foreign keys to user --
;; sender and receiver -- allowing us to test situations where multiple joins for a *single* table should occur.
(di/def-database-definition-edn avian-singles)

;; A small dataset that includes an integer column with some NULL and ZERO values, meant for testing things like
;; expressions to make sure they behave correctly
;;
;; As an added "bonus" this dataset has a table with a name in a slash in it, so the driver will need to support that
;; correctly in order for this to work!
(di/def-database-definition-edn daily-bird-counts)

(defn- calendar-with-fields ^Calendar [date & fields]
  (let [cal-from-date  (doto (Calendar/getInstance (TimeZone/getTimeZone "UTC"))
                         (.setTime date))
        blank-calendar (doto (.clone cal-from-date)
                         .clear)]
    (doseq [field fields]
      (.set blank-calendar field (.get cal-from-date field)))
    blank-calendar))

(defn- date-only
  "This function emulates a date only field as it would come from the JDBC driver. The hour/minute/second/millisecond
  fields should be 0s"
  [date]
  (.getTime (calendar-with-fields date Calendar/DAY_OF_MONTH Calendar/MONTH Calendar/YEAR)))

(defn- time-only
  "This function will return a java.sql.Time object. To create a Time object similar to what JDBC would return, the time
  needs to be relative to epoch. As an example a time of 4:30 would be a Time instance, but it's a subclass of Date,
  so it looks like 1970-01-01T04:30:00.000"
  [date]
  (Time. (.getTimeInMillis (calendar-with-fields date Calendar/HOUR_OF_DAY Calendar/MINUTE Calendar/SECOND))))

(di/def-database-definition test-data-with-time
  (di/update-table-def "users"
                       (fn [table-def]
                         [(first table-def)
                          {:field-name "last_login_date", :base-type :type/Date}
                          {:field-name "last_login_time", :base-type :type/Time}
                          (peek table-def)])
                       (fn [rows]
                         (mapv (fn [[username last-login password-text]]
                                 [username (date-only last-login) (time-only last-login) password-text])
                               rows))
                       (for [[table-name :as orig-def] (di/slurp-edn-table-def "test-data")
                             :when (= table-name "users")]
                         orig-def)))

(di/def-database-definition test-data-with-null-date-checkins
  (di/update-table-def "checkins"
                       #(vec (concat % [{:field-name "null_only_date" :base-type :type/Date}]))
                       (fn [rows]
                         (mapv #(conj % nil) rows))
                       (di/slurp-edn-table-def "test-data")))

(di/def-database-definition test-data-with-timezones
  (di/update-table-def "users"
                       (fn [table-def]
                         [(first table-def)
                          {:field-name "last_login", :base-type :type/DateTimeWithTZ}
                          (peek table-def)])
                       identity
                       (di/slurp-edn-table-def "test-data")))

(def test-data-map
  "Converts data from `test-data` to a map of maps like the following:

   {<table-name> [{<field-name> <field value> ...}]."
  (reduce (fn [acc {:keys [table-name field-definitions rows]}]
            (let [field-names (mapv :field-name field-definitions)]
              (assoc acc table-name
                     (for [row rows]
                       (zipmap field-names row)))))
          {} (:table-definitions test-data)))

(defn field-values
  "Returns the field values for the given `TABLE` and `COLUMN` found
  in the data-map `M`."
  [m table column]
  (mapv #(get % column) (get m table)))

;; Takes the `test-data` dataset and adds a `created_by` column to the users table that is self referencing
(di/def-database-definition test-data-self-referencing-user
  (di/update-table-def "users"
                       (fn [table-def]
                         (conj table-def {:field-name "created_by", :base-type :type/Integer, :fk :users}))
                       (fn [rows]
                         (mapv (fn [[username last-login password-text] idx]
                                 [username last-login password-text (if (= 1 idx)
                                                                      idx
                                                                      (dec idx))])
                               rows
                               (iterate inc 1)))
                       (for [[table-name :as orig-def] (di/slurp-edn-table-def "test-data")
                             :when (= table-name "users")]
                         orig-def)))
