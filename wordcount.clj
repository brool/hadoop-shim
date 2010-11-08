;; Federal Law requires a word count example with any Hadoop discussion.
(ns user
    (:use [clojure.contrib.io :only (read-lines)])
    (:import java.util.StringTokenizer))

;; Given an input line, return a list of key/value pairs.
(defn mapper [v]
  (let [words (enumeration-seq (StringTokenizer. v))]
    (map #(do [ % "1" ] ) words)
))

;; Given a key and a list of all values associated with that key, return the reduced value.
(defn reducer [k v]
  [ k (reduce + (map #(Integer/parseInt %) v)) ])

    (defn mapper [v]
        (let [[timehit userid] (.split v ",")]
            [ [ userid timehit ] ]
    ))

    (defn reducer [k v]
        [ k (min v) ])

;; 
;; Handy debugging functions
;;

(defn test-mapper [fname]
  (apply concat (map mapper (read-lines fname))))

(defn test-reducer [fname]
  (let [m (test-mapper fname)
        g (merge (sorted-map) (group-by #(first %) m))
        values (fn [v] (map #(nth % 1) v))]
    (map (fn [[k v]] (reducer k (values v))) g)
))

