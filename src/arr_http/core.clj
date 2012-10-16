(ns arr-http.core (:use conduit.core arrows.core arrows-extra.core) (:require  [oauth.client :as oauth] [clojure.contrib.string :as str] [oauth.signature :as sig]) (:import [java.io ByteArrayInputStream BufferedReader InputStreamReader ByteArrayInputStream][java.net Socket URLEncoder InetSocketAddress] [org.apache.commons.io.input CountingInputStream] [java.nio.channels Channel Channels SocketChannel] [java.nio ByteBuffer] [java.util Arrays] [javax.net.ssl SSLSocket SSLSocketFactory] [org.apache.http.impl.io ChunkedInputStream][rojat.io LineReaderInputStream FixedLengthInputStream] ) (:use clojure.contrib.base64))




(defn request->buffer [request buffer]
  (let [;s (apply str request_seq)
       ; encoder (. charset newEncoder)
                                        ; charBuf (CharBuffer/wrap s)]
        b (.getBytes request)
       
        ]
            

     (.flip (.put buffer b))
   
    )
)
(defn write-to-socket [socket buffer]
  "Write buffer to the socket"
   (let [channel (if (isa? socket Channel) socket (Channels/newChannel (.getOutputStream socket)))  ]

      (loop []
     
     (.write channel buffer)
    ; (println (.position msg))
     (when (.hasRemaining buffer)
       (recur)
       )
     )
      (if (isa? socket Socket) (.flush (.getOutputStream socket)))
      
     )
   
   )






    






 
(defn close [socket]
  (. socket close)

  )

(defn connect [server secure blocking]
   "If not non secure non blocking, retusn a channel, otherwise returns a socke"
   (cond (and (not secure) (not blocking)) (let [address (InetSocketAddress. (:host server) (:port server))
         socket  (. (SocketChannel/open) configureBlocking blocking)
        a (. socket connect address)
         ]
     (if a socket
         (loop []
           (if (.finishConnect socket)
             socket
             
             (recur))
         )
        
     )
     )
         blocking (let [address (InetSocketAddress. (:host server) (:port server))
          socket (if secure (.createSocket (SSLSocketFactory/getDefault) )  (Socket.))
                                    c (.connect socket address)
        ]
                                socket)
        :else (throw "Secure non blocking not implemented")
        )

  
 
   )

(defmacro arr-httpstream [socket]
  "[request] -> [instream]"
  `(a-arr (fn [request#]
            (let [buffer# (ByteBuffer/allocate (* 16 1024))
       req_buf# (request->buffer request# buffer#)
       in# (if (isa? ~socket Channel) (Channels/newInputStream ~socket) (.getInputStream ~socket))
       w# (write-to-socket ~socket req_buf#)]
  in#
  )))
  )

(def-arr arr-inline-httpstream [socket request-fn]

  (let [buffer (ByteBuffer/allocate (* 16 1024))
        req_buf (request->buffer (request-fn) buffer)
        in (if (isa? ~socket Channel) (Channels/newInputStream ~socket) (.getInputStream ~socket))
         w (write-to-socket ~socket req_buf)]
  
  in)

  )


(def-arr arr-connect [[server secure blocking]]
":: [{:host host :port port} https blocking] -> [socket]"
  (connect server secure blocking)
)

  


(defmacro switch-on-code [pred a b]
  "Eventually, this will switch on the http code e.g. to an exception pipeline"
  (a-arr (fn [[code headers in]] [headers in]))
  )



;some arrows which compose to assemble a request

(def-arr arr-append-version [url]

  (str url " HTTP/1.1")

  )


(def-arr arr-wrap-headers [header-fn]
"[{:name value .. }] -> [['name: value']]"
(reduce (fn [c [n v]] (str c (name n) ": " v "\n")) "" (seq (header-fn))

        )
)
(def-arr arr-wrap-query-params [input]
 (if input
 (reduce (fn [c [n v]] (str c (URLEncoder/encode (name n)) "=" (URLEncoder/encode v) "&")) "" (seq input)) input)

 )

(def-proc arr-apply-query-params-to-url [[authentication credentials method url header-fn body-fn query-params]]
  
  [[authentication credentials method (if (and query-params (not (empty? query-params))) (str url "?" query-params) url) header-fn body-fn query-params]]
)



(def-proc arr-content-length [[authentication credentials method url header-fn body-fn query-params]]

                                   (let [l (. (body-fn) length)
                                        
                                         header1-fn (fn [] (assoc (header-fn) :Content-Length l))

                                         ]
                                      [[authentication credentials method url header1-fn body-fn query-params]]
                                     )
                                   )

(def-proc arr-basic-authentication [[credentials method url header-fn body-fn query-params]]

  (let [user (:user credentials)
        password (:password credentials)
        userpass (str user ":" password)
        userpassenc (encode-str userpass)
        
        headers1-fn (fn [] (assoc (header-fn) :Authorization (str "Basic " userpassenc)))]
    [[method url headers1-fn body-fn query-params]]
    
        
    )

  )

(def-proc arr-oauth-authentication [[credentials method url header-fn body-fn query-params]]

  (let [creds (oauth/credentials (:consumer credentials) (:token credentials) (:token_secret credentials)  (:signature_method credentials) url query-params)
       
        header (oauth/authorization-header credentials)
        headers1-fn (fn [] (assoc (header-fn) :Authorization header))

        ]
    [[method url headers1-fn body-fn query-params]]
    )

  )
(def-proc arr-noauth[[credentials method url header-fn body-fn query-params]]

  [[method url header-fn body-fn query-params]]
  )

(def arr-authentication

  (a-comp
   (a-arr (fn [[authentication & rst]] [authentication (map identity rst)]))
   (a-select :basic arr-basic-authentication :oauth arr-oauth-authentication :none arr-noauth))

  )

;[authentication credentials method url header-fn body-fn query-params] ->
;[method url header-fn body-fn  query-params]
(def arr-process-request

  (a-comp  (a-par
            pass-through
            pass-through
            pass-through
            pass-through
            
            pass-through
            pass-through
            arr-wrap-query-params
            )
            arr-apply-query-params-to-url
          
           (a-par
            pass-through
            pass-through
            pass-through
            arr-append-version
            pass-through
          
            pass-through
            pass-through
            )
           arr-content-length
           
           arr-authentication
           (a-par
            pass-through
            pass-through
            arr-wrap-headers
            pass-through
            pass-through)
            
            
          
          )

  )


(def-arr arr-stringify-request[[method url headers body-fn query-params]]
  "[method url version header-fn body-fn] -> [string]"
  (str method " " url " \n" headers "\n" (body-fn))

       )
  
;some arrows which compose to handle a response
(def-arr arr-instream-to-seq [instream]

  [(line-seq (BufferedReader. (InputStreamReader. instream))) instream]
   
)
(defn header-from-string [accum header]
  (let [headerStr (if (seq? header) (apply str header)  header)
       [k v] (. headerStr split ": ")]
  (assoc accum (keyword k) v)

  )
  )
  (def-proc arr-line-to-code-headers [[code & headers]]
    "Use with a-par."

    
    [code (reduce header-from-string {}  (take-while (fn [input] (not (= input ""))) headers))]

    )

  


  


(def-arr arr-adapt-stream [[code headers in]]
"Retro fit some adapters depending on settings in the header"
  (let [te (:Transfer-Encoding headers)
        chunked (and (not (nil? te)) (= te "chunked"))
        contentLength (:Content-Length headers)
        s (if contentLength (FixedLengthInputStream. in contentLength) in)]

    [code headers (if chunked (ChunkedInputStream. s) s)]
       
  )

    )
(def arr-exception-pipeline nil)

;[instream] -> [headers instream]
  (def process-response

    (a-comp
     arr-instream-to-seq
    
    
      
     (a-par
      arr-line-to-code-headers
      pass-through
      )
     arr-adapt-stream
    
     )

     )

 
