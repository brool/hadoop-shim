;; Federal Law requires a word count example with any Hadoop discussion.
(ns user
    (:use [clojure.contrib.io :only (read-lines)])
    (:import java.util.StringTokenizer))

;; Given an input line, return a list of key/value pairs.
(defn mapper [^String v]
  (let [words (enumeration-seq (StringTokenizer. v))]
    (map #(do [ % "1" ] ) words)
))

;; Given a key and a list of all values associated with that key, return the reduced value.
(defn reducer [^String k ^String v]
  [ k (reduce + (map #(Integer/parseInt %) v)) ])

;; 
;; Handy debugging functions
;;

(defn test-mapper [fname]
  (apply concat (map mapper (read-lines fname))))

(defn test-reducer [fname]
  (let [m (test-mapper fname)
        g (merge (sorted-map) (group-by #(first %) m))]
    (for [[k v] g]
      (reducer k (map fnext v)))
))
