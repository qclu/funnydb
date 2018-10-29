(ns jepsen.chubao
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clj-http.client :as cljclient]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [try+]]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [tests :as tests]
                    [nemesis :as nemesis]
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [knossos.linear.report :as report]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def dir                  "/export/chubaops")
(def binary               "chubaops")
(def logfile              "/export/chubaops/chubaops.log")
(def pidfile              "/export/chubaops/chubaops.pid")
(def config-file          "/export/chubaops/ps.toml")
(def log-dir              "/export/chubaops/logs/")
(def data-dir             "/export/chubaops/datas/")

(def master-addr "10.194.133.194:9917")
(def router-addr "10.194.133.194:9901")

(def space-name "jepsen_space")
(def db-name "jepsen_db")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;SUPPORT
;(defn node-idx
;  "Given a node and a test, returns the index for that particular node, based
;  on its position in the test's list of nodes."
;  [test node]
;  ; Indices should start with 1
;  (inc (.indexOf (:nodes test) node)))
;
;(defn start-ps!
;  "Launch ps on a node"
;  [test node]
;  (c/su (cu/start-daemon!
;          {:logfile logfile 
;           :pidfile pidfile
;           :chdir   dir}
;          binary
;          :-c       config-file)) 
;  (Thread/sleep 1000)
;  :started)
;
;
;
;
;
;(defn stop-ps!
;  "Kills ps"
;  [test node]
;  (c/su (cu/stop-daemon! pidfile))
;  :stopped)
;
;(def http-opts
;  "Default clj-http options"
;  {:socket-timeout 1000
;   :conn-timeout 1000
;   :throw-exceptions? true
;   :throw-entire-message? true})
;
;  [node test]
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;SUPPORT
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;NAMESIS
;(defn ps-killer
;  "Responds to :start by killing ps on random nodes, and to :stop by
;  resuming them."
;  []
;  (nemesis/node-start-stopper identity ;util/random-nonempty-subset
;                              stop-ps!
;                              start-ps!))
;
;(defn ps-fixer
;  []
;  (reify nemesis/Nemesis
;    (setup! [this test] this)
;
;    (invoke! [this test op]
;      (assoc op :value
;             (c/on-nodes test (util/random-nonempty-subset (:nodes test))
;                         (fn [test node]
;                           (if (cu/daemon-running? s/pidfile)
;                             :already-running
;                             (do (s/start-ps! test node)
;                                 :restarted))))))
;
;    (teardown! [this test])))
;
;(defn full-nemesis
;  "Can kill and restart all processes and initiate network partitions."
;  [opts]
;  (nemesis/compose
;    {{:fix-ps        :fix}   (ps-fixer)
;     {:kill-ps       :start
;      :restart-ps    :stop}  (ps-killer)
;     {:start-partition-node  :start
;      :stop-partition-node   :stop} (nemesis/partition-random-node)
;     {:start-partition-halves  :start
;      :stop-partition-halves   :stop} (nemesis/partition-random-halves)
;     {:start-partition-ring    :start
;      :stop-partition-ring     :stop} (nemesis/partition-majorities-ring)}))
;
;(defn op
;  "Construct a nemesis op"
;  [f]
;  {:type :info, :f f, :value nil})
;
;(defn full-generator
;  "Takes a nemesis specification map from the command line, and constructs a
;  generator for the given types of nemesis operations, e.g. process kills and
;  partitions."
;  [opts]
;  (->> [(when (:kill-ps? opts)
;          [(gen/seq (cycle [(op :kill-ps)
;                            (op :restart-ps)]))])
;        (when (:fix-ps? opts)
;          [(op :fix-ps)])
;        (when (:partition-node? opts)
;          [(gen/seq (cycle (map op [:start-partition-node
;                                    :stop-partition-node])))])
;        (when (:partition-halves? opts)
;          [(gen/seq (cycle (map op [:start-partition-halves
;                                    :stop-partition-halves])))])
;        (when (:partition-ring? opts)
;          [(gen/seq (cycle (map op [:start-partition-ring
;                                    :stop-partition-ring])))])]
;       (apply concat)
;       gen/mix
;       (gen/stagger (:interval opts))))
;
;(defn nemesis
;  "Composite nemesis and generator"
;  [opts]
;  {:nemesis   (full-nemesis opts)
;   :generator (full-generator opts)
;   :final-generator (->> [(when (:partition-halves opts) :stop-partition-halves)
;                          (when (:partition-ring opts)   :stop-partition-ring)
;                          (when (:partition-node opts)   :stop-partition-node)
;                          (when (:kill-ps? opts)      :restart-ps)]
;                         (remove nil?)
;                         (map op)
;                         gen/seq
;                         (gen/delay 5))})
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;NAMESIS

(defn str->long [number-string]
    ;(try (Long/parseLong number-string)    
    ;(info (type number-string))
    ;(info number-string)
    (if-not (nil? number-string)
    (try (-> number-string Long/valueOf)    
         (catch Exception e (log/error (str "parse str [" number-string "] to int failed," "exception: " (.getMessage e)))))))

(defn createDb
  [db]
  (def res (->> (str "http://" master-addr "/manage/db/create")
                (#(cljclient/post % {:form-params {:db_name db}}))))
  (if ( = (get res :status) 200)
  (str "status: " (get res :status)))
  (get res :body))
   
(defn deleteDb
  [db]
  (def res (->> (str "http://" master-addr "/manage/db/delete")
                (#(cljclient/post % {:form-params {:db_name db}}))))
  (if ( = (get res :status) 200)
  (str "status: " (get res :status)))
  (get res :body))
   
(defn deleteSpace
  [db space]
  (def res (->> (str "http://" master-addr "/manage/space/delete")
                (#(cljclient/post % {:form-params {:db_name db
                                                :space_name space}}))))
  (if ( = (get res :status) 200)
  (str "status: " (get res :status)))
  (get res :body))

(defn createSpace
  [db space pnum]
  (def schema "{\"id\":{\"type\":\"string\"},\"val\": {\"type\": \"string\"}}")
  (def res (->> (str "http://" master-addr "/manage/space/create")
                (#(cljclient/post % {:form-params {:db_name db
                                                :space_name space
                                                :partition_num pnum
                                                :space_schema schema
                                                :properties "{\"keyField\":\"id\",\"keyFunc\":\"murmur3\"}"}}))))
  (if ( = (get res :status) 200)
  (str "status: " (get res :status)))
  (get res :body))

(defn baseurl
  "form base url, like: http://ip:port/db/space"
  [db space]
  (str "http://" router-addr "/" db "/" space))

(def counter (atom 0))

(defn next-seq []
    (swap! counter inc))

(defn setdoc
  [db space docid docval]
  (let [data (json/write-str {:id (str docid) :val (str docval)})
        opseq (str (next-seq))
        res (->> (baseurl db space)
                 (#(str % "/" docid "/_create"))
                 (#(cljclient/put % {:body data 
                                     :query-params {:seq [opseq]}})))
        body (json/read-str (get res :body) :key-fn keyword)]
    (if-not (= "created" (get body :result))
      ((log/info "set result: " res)
       (log/info "op seq: " opseq "set failed: " body)
       (throw (Exception. "document set failed")))
      (get body :result))))
   
(defn getdoc
  [db space docid]
  (let [data (json/write-str {:id (str docid)})
        opseq (next-seq)]
    (try+ 
      (def res (->> (baseurl db space)
                    (#(str % "/" docid))
                    (#(cljclient/get % {:body data
                                        :query-params {:seq [opseq]}}))))
      (str->long (get (get (json/read-str (get res :body) :key-fn keyword) :_source) :val))
      (catch [:status 500] e             
        nil))))
   
(defn db
  "chubao DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing chubao" version)
        ; Remove log file first.
        (c/exec :mkdir log-dir)
        (c/exec :mkdir data-dir)
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :process-name "start"
           :chdir   dir}
          binary
          :-c config-file)

        (Thread/sleep 10000)))

    (teardown! [_ test node]
      (info node "tearing down chubao")
      (cu/stop-daemon! binary pidfile)
      (c/su
        (c/exec :rm :-rf data-dir)               
        (c/exec :rm :-rf log-dir)))               

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(defn client
  "A client for a single compare-and-set register"
  [conn]
  (reify client/Client
    (open! [_ test node]
      (client (baseurl db-name space-name)))

    (setup! [this test]
      (try+
        (createDb db-name)
        (createSpace db-name space-name 16)
        (Thread/sleep 50000)
        
        (catch java.net.UnknownHostException e
          (log/error "UnknownHost" e))

        (catch java.net.SocketTimeoutException e
          (log/error "Timeout" e))

        (catch [:errorCode 100] e
          (log/error "NotFound statuscode 100" e))

        (catch [:body "command failed to be committed due to node failure\n"] e
          (log/error "node-failure" e))

        (catch [:status 502] e
          (log/error "Status: 502" e))

        (catch [:status 307] e
          (log/error "Status 307, redirect-loop" e))))

      
    (invoke! [this test op]
      (let [[k v] (:value op)
            crash (if (= :read (:f op)) :fail :info)]
        ;(info "key: " k "val:" v "conn: " conn)
        (try+
         (case (:f op)
           :read (let [value (->
                               (getdoc db-name space-name k))]
                   (assoc op :type :ok, :value (independent/tuple k value)))

           :write (do (setdoc db-name space-name k v)
                      (assoc op :type, :ok)))

         (catch java.net.UnknownHostException e
           (assoc op :type crash, :error e))

         (catch java.net.SocketTimeoutException e
           (assoc op :type crash, :error :timeout))

         (catch [:errorCode 100] e
           (assoc op :type :fail, :error :not-found))

         (catch [:body "command failed to be committed due to node failure\n"] e
           (assoc op :type crash, :error :node-failure))

         (catch [:status 500] e
           (assoc op :type crash, :error e))

         (catch [:status 502] e
           (assoc op :type crash, :error e))

         (catch [:status 307] e
           (assoc op :type crash, :error :redirect-loop))

         (catch (and (instance? clojure.lang.ExceptionInfo %)) e
           (assoc op :type crash, :error e))

         (catch Exception e 
           (assoc op :type :fail, :error :set-failed))

         (catch (and (:errorCode %) (:message %)) e
           (assoc op :type crash, :error e)))))

    ; If our connection were stateful, we'd close it here.
    ; Verschlimmbesserung doesn't hold a connection open, so we don't need to.
    (close! [_ _])

    (teardown! [_ _])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn chubao-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         {:name "chubao"
          :os debian/os
          :db (db "tig-chubao")
          :client (client nil)
          :nemesis (nemesis/partition-random-halves)
          ;:nemesis (nemesis opts)
          :model  (model/cas-register)
          :checker (checker/compose
                     {:perf     (checker/perf)
                      :indep (independent/checker
                               (checker/compose
                                 {:timeline (timeline/html)
                                  :linear   (checker/linearizable)}))})
          :generator (->> (independent/concurrent-generator
                            10
                            (range)
                            (fn [k]
                              (->> (gen/mix [r w])
                                   (gen/stagger 1/30)
                                   (gen/limit 900))))
                          (gen/nemesis 
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn chubao-test})
                   (cli/serve-cmd))
            args))

;(defn -main 
;  "Handles command line arguments. Can either run a test, or a web server for
;  browsing results."
;  [& args]
;  (try+
;    ;(log/info (str "delete space:" space-name))
;    ;(def res (deleteSpace db-name space-name))
;    ;(log/info (str "delete space result: " res))
;    ;(log/info (str "delete db:" db-name))
;    ;(def res (deleteDb db-name))
;    ;(log/info (str "delete db result: " res))
;    ;(log/info (str "create db: " db-name))
;    ;(def res (createDb db-name))
;    ;(log/info (str "create db result: " res))
;    ;(log/info (str "create space:" space-name))
;    ;(def res (createSpace db-name space-name 16))
;    ;(log/info (str "create space result: " res))
;    (log/info (str "create doc:" ))
;    (def res (setdoc db-name space-name 10 20))
;    (log/info (str "create doc result: " res))
;    (def res (getdoc db-name space-name 10))
;    (log/info (str "get doc result: " res))
;    (catch [:status 500] {:keys [request-time headers body]}
;      (log/error "status: 500" "error:" body))
;    (catch [:status 405] {:keys [request-time headers body]}
;      (log/error "status: 405" "error:" body "header: " headers))
;    (catch [:status 404] {:keys [request-time headers body]}
;      (log/error "status: 404" "result:" body))
;    (catch Exception e (log/error (str "exception: " (.getMessage e))))))
