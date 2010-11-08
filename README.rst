Hadoop Shim
===========

A hacky way of writing quick Hadoop 0.20 tasks in Clojure.  This is
just a tiny shim that takes a Clojure file with a "mapper" and
"reducer" function and uses it as a Hadoop Map/Reduce task.  The
advantage of using the shim is that there is no build / JAR step;  one
JAR will work for any one-file Clojure Hadoop task.

Getting The Jar
---------------

The quickest way of downloading the .JAR is to just get it from:

    http://images.brool.com/blog/files/coding/shim.jar

Building The Jar 
----------------

Build the shim::

    javac -cp /usr/lib/hadoop/hadoop-core.jar:/usr/lib/hadoop/lib/*:lib/* -d classes Shim.java

Create a lib directory and add the clojure classes::

    mkdir lib
    cp /from/wherever/clojure-1.2.0.jar lib
    cp /from/wherever/clojure-contrib-1.2.0.jar lib

Bundle it together into one .JAR file::

    jar -cvf shim.jar -C classes/ . lib/*

The directory of your .JAR should look something like this when you've
finished::

    [~/github/hadoop-shim] $ jar tf shim.jar
    META-INF/
    META-INF/MANIFEST.MF
    com/
    com/brool/
    com/brool/Shim.class
    com/brool/Shim$Reduce.class
    com/brool/Shim$Map.class
    lib/clojure-1.2.0.jar
    lib/clojure-contrib-1.2.0.jar

Using The Jar
-------------

To run the example wordcount::

    hadoop jar shim.jar com.brool.Shim -files wordcount.clj input-file output-file

The output from the run will be in output-file/part-r-00000

More Details
------------

The Clojure file that you provide should be in the user namespace, and
must provide two functions named `mapper` and `reducer`.  All inputs
and outputs are considered to be strings.

Mapper
------

The mapper function takes a string representing one line in the input
file and returns a list of [ key value ] pairs.  A given input line
can generate any number of map lines.

Reducer
-------

The reducer is given a key and all values that were associated with
that key.  The reducer's function is to consolidate all of that into
one output line.

Example
-------

Since word count is the canonical example, let's do that.  The mapper
for word count is::

    (defn mapper [v]
      (let [words (enumeration-seq (StringTokenizer. v))]
        (map #(do [ % "1" ] ) words)
    ))

As an example, for an input line of "This is a test, is it not?", the
following map will be generated::

    (["This" "1"] ["is" "1"] ["a" "1"] ["test," "1"] ["is" "1"] ["it" "1"] ["not?" "1"])

Before being given the reducer, the Hadoop system will group all like
keys together, resulting in::

    "This" => [ "1" ]
    "is"   => [ "1" "1" ]

So on and so forth.  The reducer is given the key and the list of all
values that had that key, and then emits the final result -- for a
word count, the correct code would be::

    (defn reducer [k v]
      [ k (reduce + (map #(Integer/parseInt %) v)) ])

This simply adds up the counts.

Testing
-------

In wordcount.clj there are two handy functions that allow for
debugging of the mapper and reducer before submitting it to Hadoop.

Given a local file, the `test-mapper` will load the file and run it
through the mapper;  if your file is large you may just want to `(take
20 (test-mapper "/my/filename"))`.

The `test-reducer` function will load the file and run it through both the
mapper and the reducer.  Taking the example sentence above::

    user> (test-reducer "/tmp/one-sentence")
    (["This" 1] ["a" 1] ["is" 2] ["it" 1] ["not?" 1] ["test," 1])

Example #2
----------

As another example: let's say that you have a collection of log
entries, and would like to record the first and last log entry for
every user.  Assume that the files are in a CSV format, with the
fields being in the order of timehit, userid.  Example::

    2010-10-04 13:04:22,112334
    2010-10-04 10:04:22,182994
    2010-10-04 10:05:18,182994
    2010-10-04 10:07:19,182994
    2010-10-04 13:28:41,112334
    2010-10-04 10:09:22,182994
    2010-10-04 13:56:22,112334
    2010-10-04 11:30:01,182994

The mapper for this::

    (defn mapper [v]
        (let [[timehit userid] (.split v ",")]
            [ [ userid timehit ] ]
    ))

The reducer::

    (defn reducer [k v]
      (let [s (sort v)]
        [k (str (first s) "," (last s))]))

We can test them easily::

    user> (test-mapper "/tmp/time-lists")
    (["112334" "2010-10-04 13:04:22"] ["182994" "2010-10-04 10:04:22"] ["182994" "2010-10-04 10:05:18"] ["182994" "2010-10-04 10:07:19"] ["112334" "2010-10-04 13:28:41"] ["182994" "2010-10-04 10:09:22"] ["112334" "2010-10-04 13:56:22"] ["182994" "2010-10-04 11:30:01"])
    user> (test-reducer "/tmp/time-lists")
    (["112334" "2010-10-04 13:04:22,2010-10-04 13:56:22"] ["182994" "2010-10-04 10:04:22,2010-10-04 11:30:01"])

When run as a Hadoop task, the final output will be something like::

    112334       2010-10-04 13:04:22,2010-10-04 13:56:22
    182994       2010-10-04 10:04:22,2010-10-04 11:30:01
    


