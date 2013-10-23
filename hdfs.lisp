(in-package :cl-user)
(defpackage :nellie.hdfs
  (:nicknames #:hdfs)
  (:use :cl :alists :simple-date-time :iterate :alexandria)
  (:import-from :metabang-bind :bind)
  (:import-from :flexi-streams
                :make-flexi-stream :octets-to-string
                :with-output-to-sequence :octet)
  (:export #:+webfs-port+
           #:+webhdfs-port+
           #:*default-port*
           #:*namenode-rpc-address*
           #:make-context
           #:create
           #:append-to
           #:mkdirs
           #:list-status
           #:get-file-status
           #:delete-file-or-dir
           #:from-hadoop-time
           #:server-error
           #:server-error-code
           #:server-error-message))

(in-package :nellie.hdfs)

; http://hadoop.apache.org/docs/r1.2.1/webhdfs.html

(define-constant +webfs-port+ 14000)
(define-constant +webhdfs-port+ 50075)

(defparameter *default-port* +webfs-port+)
(defparameter *namenode-rpc-address* "localhost:8020")

(defstruct context
  (host "127.0.0.1")
  (port *default-port*)
  (user "hdfs"))

(define-condition server-error (error)
  ((type :initarg :type :reader error-type)
   (code :initarg :code :reader error-code)
   (message :initarg :message :reader error-message)))

(defmethod print-object ((object server-error) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "[~D ~A]: ~A"
            (error-type object)
            (error-code object)
            (error-message object))))

(defun server-error (type code message)
  (error 'server-error
         :type type
         :code code
         :message message))

(defun add-url-parameters (url parameters-alist)
  (etypecase url
    (string (url-merge-parameters (puri:parse-uri url) parameters-alist))
    (puri:uri
     (let* ((query-string (puri:uri-query url))
            (query-params (drakma::dissect-query query-string))
            (all-params (alist-merge (list query-params parameters-alist)
                                     :test #'equal)))
       (setf (puri:uri-query url)
             (drakma::alist-to-url-encoded-string all-params
                                                  drakma:*drakma-default-external-format*
                                                  #'drakma:url-encode))
       url))))

(defun copy-stream (out in &key (blocksize 4096) progress-callback)
  (let ((buf (make-array blocksize :element-type (stream-element-type in))))
    (iter (for size = (read-sequence buf in))
          (while (plusp size))
          (sum size into total)
          (write-sequence buf out :end size)
          (when progress-callback
            (for data
                 first nil
                 then (funcall progress-callback total data))))))

(defun slurp-stream (stream)
  (with-output-to-sequence (out :element-type 'octet)
    (copy-stream out stream)))

(defun slurp-stream-as-string (stream)
  (octets-to-string (slurp-stream stream)))

(defun ignore-content (stream)
  (declare (ignore stream))
  nil)

(defun decode-json (stream)
  (let ((string (slurp-stream-as-string stream)))
    (when (plusp (length string))
      (json:decode-json-from-string string))))

(defparameter *decoder-map* `(("application/json" . ,#'decode-json)
                              ("text/plain" . ,#'slurp-stream-as-string)
                              ("text/html" . ,#'slurp-stream-as-string)))

(defun ignore-response (stream headers)
  (declare (ignore stream headers))
  nil)

(defun decode-response (stream headers)
  (let* ((content-type (drakma:header-value :content-type headers))
         (decoder (or (alist-get-str *decoder-map* content-type)
                      #'ignore-content)))
    (funcall decoder stream)))

(defun handle-error (response-stream headers status-code reason-phrase)
  (let* ((response (decode-response response-stream headers))
         (content-type (drakma:header-value :content-type headers))
         (exception (if (equal content-type "application/json")
                        (alist-get response :*remote-exception)
                        `((:exception . nil)
                          (:message . response))))
         (name (alist-get exception :exception))
         (type (if name
                   (make-keyword (cl-json::simplified-camel-case-to-lisp name))
                   :server-error))
         (message (or (alist-get exception :message)
                      reason-phrase
                      "Unknown error")))
    (error 'server-error
           :type type
           :code status-code
           :message message)))

(defun unexpected-redirect (location)
  (error "Unexpected redirect to ~S" location))

(defun send-request (method url
                     &key
                       parameters content (content-length t)
                       response-handler redirect-handler)
  (bind ((drakma:*header-stream* nil)
         ((:values response-stream status headers
                   _ _ must-close reason-phrase)
          (drakma:http-request (add-url-parameters url parameters)
                               :preserve-uri t
                               :external-format-in :utf-8
                               :external-format-out :utf-8
                               :force-binary t
                               :method method
                               :redirect nil
                               :content content
                               :content-length content-length
                               :content-type "application/octet-stream"
                               :want-stream t))
         (response-handler (or response-handler #'decode-response))
         (redirect-handler (or redirect-handler #'unexpected-redirect)))
    (unwind-protect
         (cond ((<= 200 status 201) (funcall response-handler
                                             response-stream headers))
               ((= status 307) (funcall redirect-handler
                                        (drakma:header-value :location headers)))
               ((< status 400) (error "Unexpected status code ~S" status))
               (t (handle-error response-stream
                                headers status reason-phrase)))
      (when must-close
        (close response-stream)))))

(defun send-operation-request (context method operation path
                               &key parameters
                                 response-handler
                                 redirect-handler)
  (let* ((user (context-user context))
         (path (if (eq (char path 0) #\/) path
                   (format nil "/users/~A/~A" user path)))
         (url (format nil "http://~A:~D/webhdfs/v1~A"
                      (context-host context)
                      (context-port context)
                      path))
         (op-parameters `(("user.name" . ,user)
                          ("op" . ,operation)
                          ("namenoderpcaddress" . ,*namenode-rpc-address*)))
         (parameters (alist-merge (list parameters
                                        op-parameters))))
    (send-request method url
                  :parameters parameters
                  :response-handler response-handler
                  :redirect-handler redirect-handler)))

(defun open-file (context path output-stream
                  &key offset length buffersize progress-callback)
  (labels ((stream-response (response-stream headers)
             (declare (ignore headers))
             (copy-stream output-stream
                          response-stream
                          :progress-callback progress-callback)
             t)
           (location-redirect (location)
             (send-request :get location
                           :response-handler #'stream-response)))
    (send-operation-request context :get "OPEN" path
                            :parameters `(("offset" . ,offset)
                                          ("length" . ,length)
                                          ("buffersize" . ,buffersize))
                            :response-handler #'stream-response
                            :redirect-handler #'location-redirect)))

(defun send-data-request (context method operation path
                          content-stream content-length
                          &key parameters progress-callback)
  (unless (subtypep (stream-element-type content-stream) 'octet)
    (error "Expected a binary content stream"))
  (let ((length (or content-length
                    (when (typep content-stream 'file-stream)
                      (file-length content-stream))
                    (error "Can't determine the content length")))
        (wrapper (lambda (request-stream)
                   (copy-stream request-stream
                                content-stream
                                :progress-callback progress-callback))))
    (flet ((unexpected-response (response-stream headers)
             (declare (ignore response-stream headers))
             (error "Expected a redirect, got data."))
           (upload-data (location)
             (send-request method location
                           :content wrapper
                           :content-length length
                           :response-handler #'ignore-response)))
      (send-operation-request context method operation path
                              :parameters parameters
                              :response-handler #'unexpected-response
                              :redirect-handler #'upload-data)))
  t)

(defun create (context path content-stream
               &key
                 overwrite blocksize replication permission buffersize
                 content-length progress-callback)
  (let ((parameters `(("overwrite" . ,(if overwrite "true" "false"))
                      ("blocksize" . ,blocksize)
                      ("replication" . ,replication)
                      ("permission" . ,permission)
                      ("buffersize" . ,buffersize))))
    (send-data-request context :put "CREATE" path content-stream content-length
                       :parameters parameters
                       :progress-callback progress-callback)))

(defun append-to (context path content-stream
                  &key buffersize content-length progress-callback)
  (bind ((parameters `(("buffersize" . ,buffersize))))
    (send-data-request context :post "APPEND" path content-stream content-length
                       :parameters parameters
                       :progress-callback progress-callback)))

(defun from-hadoop-time (hadoop-time)
  (let ((secs (floor (/ hadoop-time 1000.0))))
    (from-posix-time secs)))

(defun list-status (context path)
  (let* ((res (send-operation-request context :get "LISTSTATUS" path))
         (statuses (alist-get (alist-get res :*file-statuses) :*file-status)))
    (iter (for status in statuses)
          (collect
              (let ((mtime (alist-get status :modification-time))
                    (atime (alist-get status :access-time))
                    (type  (alist-get status :type)))
                (alist-merge `(,status
                               ((:modification-time . ,(from-hadoop-time mtime))
                                (:access-time . ,(from-hadoop-time atime))
                                (:type . ,(make-keyword type))))))))))

(defun get-file-status (context path)
  (let ((response (send-operation-request context :get "GETFILESTATUS" path)))
    (alist-get response :*file-status)))

(defun mkdirs (context path &key permission)
  (bind ((parameters `(("permission" . ,permission)))
         (response (operation-request context :put "MKDIRS" path
                                      :parameters parameters)))
    (alist-get response :boolean)))

(defun delete-file-or-dir (context path &key recursive)
  (let* ((parameters `(("recursive" . ,(if recursive "true" "false"))))
         (response (send-operation-request context :delete "DELETE" path
                                           :parameters parameters
                                           :response-handler #'ignore-response)))
    (alist-get response :boolean)))
